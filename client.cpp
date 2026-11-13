// ======================= TIME-TRAVEL DEBUGGER - CLIENT TEMPLATE =======================

// The client never talks to the server directly. It only talks to the load balancer.
// The load balancer wakes the server up if it is down, so the client does not care.
// The first "start" may just take a bit longer.
//
// The network is used ONLY by "start". After the .tdbg file is downloaded,
// next / prv / jmp read that file on the local disk. They never contact the load balancer.
//
// How the pieces fit together:
//   main loop -> readCommand -> runCommand
//   start -> cmdStart()  -> requestDebugFile() (Part A) -> openTdbg() (Part B) -> shows step 1
//   next, prv, jmp -> moveBy() (Part D) -> readSnapshotAt() (Part B) -> printSnapshot() (Part E)

#include <iostream>
#include <string>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <signal.h>
#include <vector>
#include <sys/socket.h>
#include "helper.h"
#include <arpa/inet.h>
#include <cerrno>
#include <limits>
#include <netinet/in.h>
#include <sstream>
using namespace std;

// ---- Constants ----
const char* PROGRAM_NAME = "ttdb";
const char* PROMPT = "ttdb> ";
const int IO_BUFFER_SIZE = 64 * 1024;     // fixed buffer for streaming files to and from the socket
const char* TDBG_FILE = "ttdb_last.tdbg"; // where the downloaded .tdbg is saved
const int MAX_JUMP = 1000000000;          // biggest n accepted by jmp
string LB_HOST = "127.0.0.1";
int LB_PORT = 5000;

// These two must be the same as in Phase 1

const int MAX_VARS_PER_FRAME = 16;
const int MAX_STACK_DEPTH = 64;


// ---- Structs (same as Phase 1, so the file can be read back exactly as it was written) ----
struct Variable
{
    string name;
    int32_t value;
};
struct Frame
{
    string func_name;
    int32_t argc;
    Variable argv[MAX_VARS_PER_FRAME];
    int64_t returnLine;
    Variable locals[MAX_VARS_PER_FRAME];
    int32_t localCount;
};
struct Snapshot
{
    Frame callStack[MAX_STACK_DEPTH];
    int32_t stackDepth;
};
struct TTDBHeader
{
    char magic[4]; // "TTDB"
    int32_t version;
    int32_t stepCount;
    int64_t indexOffset;
};

// ---- Structs only the client needs ----
struct TdbgReader
{
    FILE* file_index;
    TTDBHeader header;
    vector<int64_t> index;
};
struct DebugSession
{
    int active_flag;     // false until a "start" has worked
    TdbgReader reader;   // the .tdbg file stays open for the whole session
    int64_t currentStep;
};

// =============================================================================================
// PART A: TALKING TO THE LOAD BALANCER
// =============================================================================================

bool recvAll(int sock, char* buf, size_t n)
{
    // recv() can return fewer bytes than asked for, so keep calling it until exactly n bytes are read.
    // Return false if the connection closes or fails before n bytes arrive.
    size_t total = 0;

    while (total < n)
    {
        ssize_t received = recv(sock, buf + total, n - total, 0);

        if (received <= 0)
        {
            return false;
        }

        total += received;
    }

    return true;
}
bool sendAll(int sock, const char* buf, size_t n)
{
    // send() can send fewer bytes than asked for, so keep calling it until all n bytes are sent.
    // Use the flag MSG_NOSIGNAL. Return false if the connection fails.
    size_t total = 0;

    while (total < n)
    {
        ssize_t sent = send(sock, buf + total, n - total, MSG_NOSIGNAL);

        if (sent <= 0)
        {
            return false;
        }

        total += sent;
    }

    return true;
}

int connectToHost(const char* host, int port)
{
    // Create a TCP socket and connect it to host:port.
    // Return the socket, or -1 if the connection fails.
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock < 0)
    {
        return -1;
    }

    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &serverAddress.sin_addr) <= 0)
    {
        close(sock);
        return -1;
    }

    if (connect(sock, (sockaddr*)&serverAddress, sizeof(serverAddress)) < 0)
    {
        close(sock);
        return -1;
    }

    return sock;
}
uint32_t updateCRC32(uint32_t crc, const char* data, size_t n)
{
    for (size_t i = 0; i < n; i++)
    {
        crc ^= (unsigned char)data[i];

        for (int j = 0; j < 8; j++)
        {
            if (crc & 1)
                crc = (crc >> 1) ^ 0xEDB88320;
            else
                crc >>= 1;
        }
    }

    return crc;
}
bool uploadSource(int sock, const char* sourcePath)
{
    // have to do checksum and then send
    // When the file is finished, call shutdown(sock, SHUT_WR). This is how the server knows the string is complete.
    // Return false if the file cannot be opened or any send fails.
    FILE* file = fopen(sourcePath, "rb");

    if (file == nullptr) {
        return false;
    }

    uint32_t crc = 0xFFFFFFFF;
    char buffer[IO_BUFFER_SIZE];

 
    char status = 0;

    if (!sendAll(sock, &status, 1))
    {
        fclose(file);
        return false;
    }

    crc = updateCRC32(crc, &status, 1);

  
    size_t bytesRead;

    while ((bytesRead = fread(buffer, 1, sizeof(buffer), file)) > 0)
    {
        crc = updateCRC32(crc, buffer, bytesRead);

        if (!sendAll(sock, buffer, bytesRead))
        {
            fclose(file);
            return false;
        }
    }

    if (ferror(file))
    {
        fclose(file);
        return false;
    }

    fclose(file);

  
    crc = ~crc;

   
    char checksum[4];

    checksum[0] = crc & 0xFF;
    checksum[1] = (crc >> 8) & 0xFF;
    checksum[2] = (crc >> 16) & 0xFF;
    checksum[3] = (crc >> 24) & 0xFF;

    if (!sendAll(sock, checksum, 4)) {
        return false;
    }

   
    shutdown(sock, SHUT_WR);

    return true;

}
bool receiveResponse(int sock, const char* outTdbgPath, string& outError)
{
    // Reply on the wire: [status][payload][CRC-32]. The CRC covers the status byte too.
    // Feed every received byte (status included) to a TrailerChecker.
    // Status 1: collect the payload into outError. Status 0: write the payload to the file.
    // At the end call verify(). If it fails, delete the file, set outError to
    // "checksum mismatch, response corrupted" and return false.
    uint32_t crc = 0xFFFFFFFF;

    char buffer[IO_BUFFER_SIZE];
    char lastFour[4];
    int pending = 0;

    char status = -1;
    FILE* file = nullptr;

    outError.clear();

    while (true)
    {
        ssize_t received = recv(sock, buffer, sizeof(buffer), 0);

        if (received < 0)
        {
            if (errno == EINTR) {

                continue;
            }

            if (file != nullptr) {
                fclose(file);
            }

            return false;
        }

        if (received == 0) {
            break;
        }

        for (ssize_t i = 0; i < received; i++)
        {
            char current = buffer[i];

            if (pending < 4)
            {
                lastFour[pending++] = current;
                continue;
            }

            char safeByte = lastFour[0];

            for (int j = 0; j < 3; j++)
                lastFour[j] = lastFour[j + 1];

            lastFour[3] = current;

            crc = updateCRC32(crc, &safeByte, 1);

            if (status == -1)
            {
                status = (unsigned char)safeByte;

                if (status == 0)
                {
                    file = fopen(outTdbgPath, "wb");

                    if (file == nullptr)
                        return false;
                }
                else if (status != 1)
                {
                    return false;
                }
            }
            else if (status == 0)
            {
                if (fwrite(&safeByte, 1, 1, file) != 1)
                {
                    fclose(file);
                    remove(outTdbgPath);
                    return false;
                }
            }
            else
            {
                outError += safeByte;
            }
        }
    }

    if (pending != 4 || status == -1)
    {
        if (file != nullptr)
            fclose(file);

        remove(outTdbgPath);
        return false;
    }

    crc = ~crc;

    uint32_t receivedCRC =
        (unsigned char)lastFour[0] |
        ((uint32_t)(unsigned char)lastFour[1] << 8) |
        ((uint32_t)(unsigned char)lastFour[2] << 16) |
        ((uint32_t)(unsigned char)lastFour[3] << 24);

    if (file != nullptr) {
        fclose(file);
    }

    if (crc != receivedCRC)
    {
        remove(outTdbgPath);
        outError = "checksum mismatch, response corrupted";
        return false;
    }

    return status == 0;
}

// =============================================================================================
// PART B: READING THE .tdbg FILE
// File layout (written by Phase 1):
//   header | snapshot 0 | snapshot 1 | ... | index (one 8 byte offset per snapshot)
// The index lets us jump straight to any step without reading the steps before it.
// =============================================================================================

bool openTdbg(const char* path, TdbgReader& r)
{
    r.file_index = nullptr;
    r.index.clear();

    FILE* file = fopen(path, "rb");

    if (file == nullptr) {
        return false;
    }

    if (fseek(file, 0, SEEK_END) != 0)
    {
        fclose(file);
        return false;
    }

    long fileSize = ftell(file);

    if (fileSize < 20)
    {
        fclose(file);
        return false;
    }

  
    if (fseek(file, 0, SEEK_SET) != 0)
    {
        fclose(file);
        return false;
    }

    if (fread(r.header.magic, 1, 4, file) != 4 ||
        fread(&r.header.version, sizeof(int32_t), 1, file) != 1 ||
        fread(&r.header.stepCount, sizeof(int32_t), 1, file) != 1 ||
        fread(&r.header.indexOffset, sizeof(int64_t), 1, file) != 1)
    {
        fclose(file);
        return false;
    }

  
    if (memcmp(r.header.magic, "TTDB", 4) != 0 ||
        r.header.version != 1 ||
        r.header.stepCount <= 0 ||
        r.header.indexOffset < 20 ||
        r.header.indexOffset > fileSize)
    {
        fclose(file);
        return false;
    }

    int64_t indexBytes = (int64_t)r.header.stepCount * sizeof(int64_t);

    if (indexBytes > fileSize - r.header.indexOffset ||
        r.header.indexOffset + indexBytes != fileSize)
    {
        fclose(file);
        return false;
    }

   
    if (fseek(file, (long)r.header.indexOffset, SEEK_SET) != 0)
    {
        fclose(file);
        return false;
    }

    r.index.resize(r.header.stepCount);

    for (int32_t i = 0; i < r.header.stepCount; i++)
    {
        if (fread(&r.index[i], sizeof(int64_t), 1, file) != 1)
        {
            r.index.clear();
            fclose(file);
            return false;
        }

        if (r.index[i] < 20 || r.index[i] >= r.header.indexOffset)
        {
            r.index.clear();
            fclose(file);
            return false;
        }

        if (i == 0 && r.index[i] != 20)
        {
            r.index.clear();
            fclose(file);
            return false;
        }

        if (i > 0 && r.index[i] <= r.index[i - 1])
        {
            r.index.clear();
            fclose(file);
            return false;
        }
    }

    r.file_index = file;
    return true;
}
void closeTdbg(TdbgReader& r)
{
   
    if (r.file_index != nullptr)
    {
        fclose(r.file_index);
        r.file_index = nullptr;
    }

    r.index.clear();
}
bool readBytes(FILE* file, void* data, size_t size, int64_t limit)
{
    long position = ftell(file);

    if (position < 0 || (int64_t)position + (int64_t)size > limit) {
        return false;
    }

    return fread(data, 1, size, file) == size;
}
bool readString(FILE* file, string& value, int64_t limit)
{
    int32_t length;

    if (!readBytes(file, &length, sizeof(int32_t), limit)) {
        return false;

    }
    if (length < 0) {
        return false;
    }
    long position = ftell(file);

    if (position < 0 || (int64_t)position + length > limit) {
        return false;
    }
    value.resize(length);

    if (length > 0)
    {
        if (!readBytes(file, &value[0], length, limit)) {
            return false;
        }
    }

    return true;
}
bool readVariable(FILE* file, Variable& variable, int64_t limit)
{
    if (!readString(file, variable.name, limit)) {
        return false;
    }

    if (!readBytes(file, &variable.value, sizeof(int32_t), limit)) {
        return false;
    }

    return true;
}
Snapshot* readSnapshotAt(TdbgReader& r, int32_t step)
{
    if (r.file_index == nullptr ||
        step < 0 ||
        step >= r.header.stepCount) {
        return nullptr;
    }
    if (r.index.size() != (size_t)r.header.stepCount) {
        return nullptr;
    }
    int64_t start = r.index[step];

    int64_t limit;

    if (step == r.header.stepCount - 1) {
        limit = r.header.indexOffset;
    }
    else {
        limit = r.index[step + 1];
    }
    if (fseek(r.file_index, (long)start, SEEK_SET) != 0) {
        return nullptr;
    }

    Snapshot* snapshot = new Snapshot{};

    if (!readBytes(r.file_index, &snapshot->stackDepth,
        sizeof(int32_t), limit))
    {
        delete snapshot;
        return nullptr;
    }

    if (snapshot->stackDepth < 0 ||
        snapshot->stackDepth > MAX_STACK_DEPTH)
    {
        delete snapshot;
        return nullptr;
    }

    for (int32_t i = 0; i < snapshot->stackDepth; i++)
    {
        Frame& frame = snapshot->callStack[i];

        if (!readString(r.file_index, frame.func_name, limit))
        {
            delete snapshot;
            return nullptr;
        }

        if (!readBytes(r.file_index, &frame.argc,
            sizeof(int32_t), limit))
        {
            delete snapshot;
            return nullptr;
        }

        if (frame.argc < 0 || frame.argc > MAX_VARS_PER_FRAME)
        {
            delete snapshot;
            return nullptr;
        }

        for (int32_t j = 0; j < frame.argc; j++)
        {
            if (!readVariable(r.file_index, frame.argv[j], limit))
            {
                delete snapshot;
                return nullptr;
            }
        }

        if (!readBytes(r.file_index, &frame.returnLine,
            sizeof(int64_t), limit))
        {
            delete snapshot;
            return nullptr;
        }

        if (!readBytes(r.file_index, &frame.localCount,
            sizeof(int32_t), limit))
        {
            delete snapshot;
            return nullptr;
        }

        if (frame.localCount < 0 ||
            frame.localCount > MAX_VARS_PER_FRAME)
        {
            delete snapshot;
            return nullptr;
        }

        for (int32_t j = 0; j < frame.localCount; j++)
        {
            if (!readVariable(r.file_index, frame.locals[j], limit))
            {
                delete snapshot;
                return nullptr;
            }
        }
    }
    long endPosition = ftell(r.file_index);

    if (endPosition < 0 || (int64_t)endPosition != limit)
    {
        delete snapshot;
        return nullptr;
    }
    return snapshot;
}

// =============================================================================================
// PART C: THE SESSION IN MEMORY
// There is one DebugSession variable, created in main() and passed to every command by reference.
// =============================================================================================

void initSession(DebugSession& s)
{
    s.active_flag = false;
    s.reader.file_index = nullptr;
    s.reader.index.clear();
    s.currentStep = 0;
}
void closeSession(DebugSession& s)
{
    // If active, call closeTdbg(s.reader). Then set active = false.
    // Called when the user types quit and when the input ends.
    if (s.active_flag)
    {
        closeTdbg(s.reader);
    }

    s.active_flag = false;
    s.currentStep = 0;
}

// =============================================================================================
// PART D: THE COMMANDS
// =============================================================================================

void printSnapshot(int32_t step, int32_t totalSteps, const Snapshot& s); // defined in Part E

bool showCurrent(DebugSession& s)
{
    // Read the snapshot at s.currentStep, call printSnapshot, delete the snapshot.
    // If readSnapshotAt returns nullptr, print "damaged .tdbg file" and return false.
    Snapshot* snapshot = readSnapshotAt(
        s.reader, (int32_t)s.currentStep
    );

    if (snapshot == nullptr)
    {
        cout << "damaged .tdbg file" << endl;
        return false;
    }

    printSnapshot(
        (int32_t)s.currentStep + 1,
        s.reader.header.stepCount,
        *snapshot
    );

    delete snapshot;

    return true;
}
bool moveBy(DebugSession& s, int32_t delta)
{
    if (!s.active_flag)
    {
        cout << "No active debugging session. Use start first." << endl;
        return false;
    }

    int64_t newStep = s.currentStep + delta;

    if (newStep < 0)
    {
        cout << "Already at the first step." << endl;
        return false;
    }

    if (newStep >= s.reader.header.stepCount)
    {
        cout << "Already at the last step." << endl;
        return false;
    }

    s.currentStep = newStep;

    return showCurrent(s);
}
bool cmdStart(DebugSession& s, const string& sourcePath)
{
    // 1. requestDebugFile with host and port, saving to TDBG_FILE.
    // 2. If it fails, print the error text and return false. Keep the old session as it was.
    // 3. Open the new file with openTdbg into a NEW local TdbgReader.
    //    If that fails, print "server sent a damaged .tdbg file", keep the old session, return false.
    // 4. Only now close the old reader (if s.active), then put the new reader into s,
    //    set s.currentStep = 0 and s.active = true.
    // 5. Show step 1 with showCurrent(s).
    const char* tempFile = "ttdb_download.tmp";

   
    int sock = connectToHost(LB_HOST.c_str(), LB_PORT);

    if (sock < 0)
    {
        cout << "Could not connect to load balancer." << endl;
        return false;
    }

    if (!uploadSource(sock, sourcePath.c_str()))
    {
        cout << "Source upload failed." << endl;
        close(sock);
        return false;
    }

  
    string errorMessage;

    bool received = receiveResponse(sock, tempFile, errorMessage);

    close(sock);

    if (!received)
    {
        if (!errorMessage.empty()) {
            cout << errorMessage << endl;
        }
        else {
            cout << "Failed to receive debugging file." << endl;
        }

        remove(tempFile);
        return false;
    }

    
    TdbgReader newReader;

    if (!openTdbg(tempFile, newReader))
    {
        cout << "server sent a damaged .tdbg file" << endl;
        remove(tempFile);
        return false;
    }

    if (rename(tempFile, TDBG_FILE) != 0)
    {
        cout << "Could not save debugging file." << endl;
        closeTdbg(newReader);
        remove(tempFile);
        return false;
    }

    
    closeSession(s);

    s.reader = newReader;
    newReader.file_index = nullptr;

    s.currentStep = 0;
    s.active_flag = true;

   
    return showCurrent(s);
}
bool cmdNext(DebugSession& s) {
    return moveBy(s, 1);
}
bool cmdPrv(DebugSession& s) {
    return moveBy(s, -1);
}
bool cmdJmpNext(DebugSession& s, int32_t n) {
    return moveBy(s, n);
}
bool cmdJmpPrv(DebugSession& s, int32_t n) {
    return moveBy(s, -n);
}

// =============================================================================================
// PART E: PRINTING
// =============================================================================================

void printSnapshot(int32_t step, int32_t totalSteps, const Snapshot& s)
{
    cout << "\n========== STEP " << step
        << " / " << totalSteps << " ==========\n";

    cout << "Stack Depth: " << s.stackDepth << "\n";

    for (int32_t i = 0; i < s.stackDepth; i++)
    {
        const Frame& frame = s.callStack[i];

        cout << "\n--- Frame " << i + 1 << " ---\n";

        cout << "Function: " << frame.func_name << "\n";

        cout << "Arguments (" << frame.argc << "):\n";

        for (int32_t j = 0; j < frame.argc; j++)
        {
            cout << "  " << frame.argv[j].name
                << " = " << frame.argv[j].value << "\n";
        }

        cout << "Return Line: " << frame.returnLine << "\n";

        cout << "Local Variables (" << frame.localCount << "):\n";

        for (int32_t j = 0; j < frame.localCount; j++)
        {
            cout << "  " << frame.locals[j].name
                << " = " << frame.locals[j].value << "\n";
        }
    }

    cout << "================================\n";
}
void printHelp()
{
  
    cout << "\nAvailable Commands:\n";

    cout << "  start <source_file>  - Start a debugging session\n";
    cout << "  next                - Move to the next step\n";
    cout << "  prv                 - Move to the previous step\n";
    cout << "  jmp next <n>        - Jump forward n steps\n";
    cout << "  jmp prv <n>         - Jump backward n steps\n";
    cout << "  help                - Display available commands\n";
    cout << "  quit                - Exit the debugger\n";
    cout << "  exit                - Exit the debugger\n\n";
}

// =============================================================================================
// PART F: THE INFINITE LOOP
// =============================================================================================

bool runCommand(DebugSession& s, const string& line, bool& quitRequested)
{
    // 1. Split the line into words.
    // 2. If the first word is "ttdb", drop it.
    // 3. Match the first word:
    //      start      -> needs a path after it, then cmdStart
    //      next       -> no more words, then cmdNext
    //      prv        -> no more words, then cmdPrv
    //      jmp        -> second word "next" or "prv", third word a positive number ,
    //                    then cmdJmpNext or cmdJmpPrv
    //      help       -> printHelp
    //      quit, exit -> set quitRequested = true
    // 4. For anything else, or a wrong number of words, print "unknown command" and the help text, and return false.
    vector<string> words;
    string word;

    istringstream input(line);

    while (input >> word)
    {
        words.push_back(word);
    }

    if (words.empty()) {
        return true;
    }

    
    if (words[0] == "ttdb")
    {
        words.erase(words.begin());

        if (words.empty()) {
            return true;
        }
    }

    if (words[0] == "start" && words.size() == 2)
    {
        return cmdStart(s, words[1]);
    }

    if (words[0] == "next" && words.size() == 1)
    {
        return cmdNext(s);
    }

    if (words[0] == "prv" && words.size() == 1)
    {
        return cmdPrv(s);
    }

    
    if (words[0] == "jmp" && words.size() == 3)
    {
        if (words[1] != "next" && words[1] != "prv")
        {
            cout << "unknown command" << endl;
            printHelp();
            return false;
        }

        char* end;
        errno = 0;

        long n = strtol(words[2].c_str(), &end, 10);

        if (errno != 0 || *end != '\0' ||
            n <= 0 || n > MAX_JUMP)
        {
            cout << "Invalid jump number." << endl;
            return false;
        }

        if (words[1] == "next") {
            return cmdJmpNext(s, (int32_t)n);
        }

        return cmdJmpPrv(s, (int32_t)n);
    }

    
    if (words[0] == "help" && words.size() == 1)
    {
        printHelp();
        return true;
    }

   
    if ((words[0] == "quit" || words[0] == "exit") &&
        words.size() == 1)
    {
        quitRequested = true;
        return true;
    }

    cout << "unknown command" << endl;
    printHelp();

    return false;
}
int main(int argc, char* argv[])
{
    // you get string LB_HOST and int LB_PORT = 0 in arguments of this main function. 

    // DebugSession session; initSession(session);
    // Print a short welcome and the help text.
    //
    // Infinite loop:
    //   if quitRequested, break
    //
    // After the loop: closeSession(session) and return 0.
    // Never exit the loop because of an error. Only quit or the end of the input leaves it.
     // Check command-line arguments
    if (argc != 3)
    {
        cout << "Usage: ./client <lb_ip> <lb_port>" << endl;
        return 1;
    }

    
    LB_HOST = argv[1];

    
    char* end;
    errno = 0;

    long port = strtol(argv[2], &end, 10);

    if (errno != 0 || *end != '\0' ||
        port <= 0 || port > 65535)
    {
        cout << "Invalid load balancer port." << endl;
        return 1;
    }

    LB_PORT = (int)port;

    
    DebugSession session;
    initSession(session);

    cout << "\nWelcome to Time-Travel Debugger!\n";

    printHelp();

    string line;
    bool quitRequested = false;

   
    while (!quitRequested)
    {
        cout << PROMPT;

        if (!getline(cin, line)) {
            break;
        }
        runCommand(session, line, quitRequested);
    }

    closeSession(session);

    cout << "Debugger closed." << endl;

    return 0;

}