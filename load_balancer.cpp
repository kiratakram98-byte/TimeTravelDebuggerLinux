// ======================= TIME-TRAVEL DEBUGGER - LOAD BALANCER TEMPLATE =======================

// What load balancer does:
//   1. Many clients can connect at the same time. Each one sends one string (the source code). 
//   2. Clients wait in a queue (first come, first served).
//   3. For each client, in order: make sure the server is running (start it with system() if not),
//      pass the string to the server, pass the server's reply back to the client.
//
// It does not read or understand the messages. It only moves bytes.
// The only messages it writes itself are errors ("server busy", "server failed to start").
//
// Two threads:
//   acceptor thread (acceptLoop) -> takes new clients and puts them in the queue. It never waits for the server.
//   main thread (dispatcherLoop) -> takes ONE client out of the queue and serves it completely, then the next.
//
// How the server is started:
//   system("./server 5001 &");
//   The "&" is required. Without it, system() waits until the server exits and the load balancer freezes.
//
//
// What the server program must do (the load balancer depends on this):
//   - it takes its port as the first argument, for example: ./server 5001
//   - it reads the string until the connection's write side is closed (recv returns 0)
//   - it sends its reply as [status: 1 byte][payload], then closes the connection 


#include <iostream>
#include <string>
#include <cstdlib>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cerrno>
#include "helper.h"
#include <stdexcept>
#include <ctime>
#include <cstdint>
#include <chrono>
#include <cstdio>
using namespace std;


// ---- Constants ----
const char* SERVER_PATH = "./server";      // the program we start when the server is down
const int IO_BUFFER_SIZE = 64 * 1024;      // fixed buffer for moving bytes
const int LISTEN_BACKLOG = 16;             // connections the OS holds until accept() takes them
const int MAX_PENDING_CLIENTS = 8;         // clients allowed to wait in our queue. More than this get "server busy".
const int STARTUP_MAX_ATTEMPTS = 50;       // after starting the server, how many times we try to connect
const int STARTUP_POLL_INTERVAL_MS = 100;  // wait between those tries (50 x 100 ms = 5 seconds in total)
int lbPort = 5000;
string serverHost = "127.0.0.1";
int serverPort = 5001;


// ---- Queue: here it holds client sockets (int). ----
// Implement it yourself, do not use std::queue.
template <typename T>
class Queue
{
    struct Node
    {
        T data;
        Node* next;
    };
    Node* front, * rear;
    int count;


public:
    // Implement these functions:
    Queue()
    {
        count = 0;
        rear = nullptr;
        front = nullptr;
    }
    ~Queue()
    {
        while (!isEmpty())
        {
            dequeue();
        }
    }
    void enqueue(const T& val)
    {
        Node* n = new Node;
        n->data = val;
        n->next = nullptr;
        if (front == nullptr){        
            front = n;
            rear = n;
        }
        else{
            rear->next = n;
            rear = n;
        }
        count++;
    }
    T dequeue()
    {
        if (isEmpty()) {
            throw runtime_error("Queue is empty");
        }
        Node* temp = front;
        T val = temp->data;
        front = front->next;
        if (front == nullptr){
            rear = nullptr;
        }
        delete temp;
        count--;

        return val;
    }
    bool isEmpty()
    {
        return count == 0;
    }
    int size()
    {
        return count;
    }
};

struct ClientInfo
{
    int sock;           // the connected socket
    string address;     // the client's IP as text, e.g. "127.0.0.1"
    int port;           // the client's port
    time_t arrivedAt;   // when accept() returned
};

// The queue is used by two threads, so every use of it must be inside queueMutex.
Queue<ClientInfo> pendingClients;
mutex queueMutex;
sig_atomic_t flag = 1;


// ---- Basic socket helpers ----
bool sendAll(int sock, const char* buf, size_t n)
{
    // Reply format: [status: 1 byte][sending_material][checksum: 4 bytes].
    // send() may send fewer bytes than asked for, so keep calling it until all n bytes are sent.
    size_t totalSent = 0;

    while (totalSent < n)
    {
        int sent = send(
            sock,
            buf + totalSent,
            n - totalSent,
            MSG_NOSIGNAL
        );

        if (sent < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return false;
        }

        if (sent == 0)
        {
            return false;
        }

        totalSent += sent;
    }

    return true;
}
uint32_t updateCRC32(uint32_t crc, unsigned char byte)
{
    crc ^= byte;

    for (int i = 0; i < 8; i++)
    {
        if (crc & 1)
            crc = (crc >> 1) ^ 0xEDB88320;
        else
            crc >>= 1;
    }

    return crc;
}
bool sendError(int sock, const string& message)
{
    // Reply format: [status: 1 byte][payload until close][checksum: 4 bytes].
    // For an error: send one byte with value 1, then the message text. 
    unsigned char status = 1;

    uint32_t crc = 0xFFFFFFFF;

    // Include the status byte in the checksum.
    crc = updateCRC32(crc, status);

    // Include every character of the error message.
    for (size_t i = 0; i < message.size(); i++)
    {
        crc = updateCRC32(crc, (unsigned char)message[i]);
    }

    crc = ~crc;

    // Send status byte.
    if (!sendAll(sock, (const char*)&status, 1))
    {
        return false;
    }

    // Send error message.
    if (!sendAll(sock, message.c_str(), message.size()))
    {
        return false;
    }

    // Convert checksum into 4 little-endian bytes.
    unsigned char checksum[4];

    checksum[0] = crc & 0xFF;
    checksum[1] = (crc >> 8) & 0xFF;
    checksum[2] = (crc >> 16) & 0xFF;
    checksum[3] = (crc >> 24) & 0xFF;

    if (!sendAll(sock, (const char*)checksum, 4))
    {
        return false;
    }
    if (shutdown(sock, SHUT_WR) < 0)
    {
        return false;
    }
    return true;
}
int createListenSocket(int port)
{
    // Create a TCP socket with SOCK_CLOEXEC(flag).
    // set SO_REUSEADDR so we can restart the load balancer right away.
    // bind() to the port on any address, then listen(LISTEN_BACKLOG).
    // If bind or listen fails, print the error and exit, because without this socket we cannot work.
    int listenSock = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

    if (listenSock < 0)
    {
        perror("socket failed");
        exit(1);
    }

    int opt = 1;

    if (setsockopt(listenSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        perror("setsockopt failed");
        close(listenSock);
        exit(1);
    }

    sockaddr_in serverAddress;

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);
    serverAddress.sin_addr.s_addr = INADDR_ANY;

    if (bind(listenSock, (sockaddr*)&serverAddress, sizeof(serverAddress)) < 0)
    {
        perror("bind failed");
        close(listenSock);
        exit(1);
    }

    if (listen(listenSock, LISTEN_BACKLOG) < 0)
    {
        perror("listen failed");
        close(listenSock);
        exit(1);
    }

    return listenSock;
}
int connectToServer(const char* host, int port)
{
    // Create a TCP socket (SOCK_CLOEXEC) and connect it to host:port.
    // Return the socket, or -1 if the connection fails 
    int serverSock = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

    if (serverSock < 0)
    {
        return -1;
    }

    sockaddr_in serverAddress;

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &serverAddress.sin_addr) <= 0)
    {
        close(serverSock);
        return -1;
    }

    if (connect(serverSock, (sockaddr*)&serverAddress, sizeof(serverAddress)) < 0)
    {
        close(serverSock);
        return -1;
    }

    return serverSock;
}

// =============================================================================================
// PART 1: WAKING UP THE SERVER IF IT IS DOWN
// =============================================================================================


bool startServer(int port)
{
    // Build the command text: SERVER_PATH + " " + port as text + "&"
    //   Example: "./server 5001 &"
    // Call system() with it.
    // Return true if system() returned 0, otherwise false.
    string command = string(SERVER_PATH) + " " + to_string(port) + " &";

    int result = system(command.c_str());

    if (result != 0)
    {
        return false;
    }

    return true;
}
int connectToServerWithWakeup()
{
    // 1. Try connectToServer(). If it works, return that socket. The server is alive.
    // 2. Otherwise call startServer(). If it fails, return -1.
    // 3. Try connectToServer() up to STARTUP_MAX_ATTEMPTS times 
    // 4. Return the first working socket, or -1 if the server never came up.
    int serverSock = connectToServer(serverHost.c_str(), serverPort);

    if (serverSock >= 0)
    {
        return serverSock;
    }

    // Backend is down. Try to start it.
    if (!startServer(serverPort))
    {
        return -1;
    }

    // Give the backend time to start, retrying the connection.
    for (int i = 0; i < STARTUP_MAX_ATTEMPTS; i++)
    {
        this_thread::sleep_for(chrono::milliseconds(STARTUP_POLL_INTERVAL_MS));

        serverSock = connectToServer(serverHost.c_str(), serverPort);

        if (serverSock >= 0)
        {
            return serverSock;
        }
    }

    return -1;
}


// =============================================================================================
// PART 2: PASSING THE STRING AND THE REPLY (one client, called by the main thread only)
// =============================================================================================
class TrailerChecker
{
    unsigned char lastFour[4];
    int count;
    uint32_t crc;

public:
    TrailerChecker()
    {
        count = 0;
        crc = 0xFFFFFFFF;
    }

    bool update(const char* data, int length)
    {
        for (int i = 0; i < length; i++)
        {
            unsigned char byte = (unsigned char)data[i];

            if (count < 4)
            {
                lastFour[count] = byte;
                count++;
            }
            else
            {
                // The oldest byte is definitely not part of the checksum.
                crc = updateCRC32(crc, lastFour[0]);

                // Shift the remaining bytes.
                lastFour[0] = lastFour[1];
                lastFour[1] = lastFour[2];
                lastFour[2] = lastFour[3];

                // Store the new byte.
                lastFour[3] = byte;
            }
        }

        return true;
    }
    int getTrailer(char* output)
    {
        if (count != 4)
        {
            return 0;
        }

        for (int i = 0; i < 4; i++)
        {
            output[i] = (char)lastFour[i];
        }

        return 4;
    }

    bool finish()
    {
        if (count != 4)
        {
            return false;
        }

        crc = ~crc;

        uint32_t receivedChecksum =
            (uint32_t)lastFour[0] |
            ((uint32_t)lastFour[1] << 8) |
            ((uint32_t)lastFour[2] << 16) |
            ((uint32_t)lastFour[3] << 24);

        return crc == receivedChecksum;
    }
};
void relayOneClient(ClientInfo& c)
{
    // 1. serverSock = connectToServerWithWakeup().
    //    If it is -1: sendError "backend server failed to start", close clientSock, return.
    // 3. Client -> server: repeat recv() from clientSock into a buffer of IO_BUFFER_SIZE and sendAll() it to serverSock.
    //    Stop when recv() returns 0 (the client finished sending) or fails.
    // 4. Call shutdown(serverSock, SHUT_WR), so the server knows the string is complete.
    // 5. Server -> client: repeat recv() from serverSock and sendAll() to clientSock.
    //    Stop when recv() returns 0 (the server sent its whole reply and closed) or fails.
    // 6. Close both sockets.
}


// =============================================================================================
// PART 3: THE TWO THREADS
// =============================================================================================


void acceptLoop(int listenSock)
{
    // Repeat: accept() a new client. 
    //  If pendingClients.size() is already MAX_PENDING_CLIENTS: unlock, sendError "server busy, try again later",
    // else
    // Lock queueMutex.
    //   close the client, and go on.
    //   Otherwise enqueue the client socket.
    // Never wait for the server here, so new clients are always accepted quickly.
    // If accept() fails, print the error and keep going (do not end the thread).
}
void dispatcherLoop()
{
    // This runs in the main thread and never ends.
    // Repeat:
    //   Lock queueMutex, take ONE client out of the queue if there is one, unlock.
    //   If we got a client: call relayOneClient(). The next client is taken out of the queue only after this one is finished.
}

void handler(int sg) {
    (void)sg;
    flag = 0;
}
int main(int argc, char* argv[])
{

    signal(SIGINT, handler); // whenever ctrl+c will be pressed to exit the program the flag wll be set so insead of using infinite loop you can loop on flag to safely close things and exit
    // 2. listenSock = createListenSocket(LB_PORT).
    // 3. Start a thread that runs acceptLoop(listenSock) and detach it.
    // 4. Call dispatcherLoop().
    return 0;
}