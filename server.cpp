// ======================= TIME-TRAVEL DEBUGGER - SERVER TEMPLATE =======================

// Pipeline this file implements, top to bottom:
//   0. Receive  -- stream the client's .trace bytes straight to source.bin on disk
//   1. Pass 0X0   -- validity check (FUNC/FUNC_END matching)
//   2. Pass 0X1   -- resolve(): copy EVERY source line into resolve.bin as [offset][size][string], then patch CALL targets.
//   3. Pass 0X2   -- execute resolve.bin: tokenize ONE line at a time, update the call stack, take a snapshot -> Timeline
//   4. Pass 0X3   -- serialize Timeline -> session.tdbg(header + snapshot records + dense index)


#include <iostream>
#include <string>
#include <cstdint>
#include <fstream>
#include <unistd.h>
#include <sys/socket.h>
#include <cstdint>
#include <cstdio>
#include <stdexcept>
using namespace std;

// ---- Constants ----
const int32_t MAX_VARS_PER_FRAME = 16;
const int32_t MAX_STACK_DEPTH = 64;
const int32_t MAX_FUNCS = 128;
const int32_t MAX_TOKENS = MAX_VARS_PER_FRAME + 2; // kW + func_name + upto 16 params/args
const int32_t MAX_PATCHES = MAX_FUNCS * 4;
const uint64_t MAX_SOURCE_BYTES = 15ULL * 1024 * 1024; // sanity cap on the declared file length
const int32_t IO_BUFFER_SIZE = 64 * 1024;                  // fixed buffer for streaming to/from disk
const int32_t SOCKET_TIMEOUT_SEC = 5;                      // TODO: apply as SO_RCVTIMEO so a deadclient can't hang the server forever

// ---- Custom data structures

// Stack: back the live Call Stack during execution
template <typename T>
class Stack
{
    struct Node
    {
        T data;
        Node* next;
    };
    Node* top;
    int32_t count;

public:
    // Implement these functions:
    Stack()
    {
        top = nullptr;
        count = 0;
    }
    void push(const T& val)
    {
        if (count >= MAX_STACK_DEPTH) {
            return;
        }
        Node* n = new Node;
        n->next = top;
        n->data = val;
        top = n;
        count++;
        // pushes the value on the stack if max limit is not reached yet.
    }
    T pop()
    {
        if (top == nullptr) {
            throw runtime_error("Stack is empty");
        }
        Node* n = top;
        T val = n->data;
        top = top->next;
        delete n;
        count--;
        return val;


        // pop the top value on the stack
    }
    T& peek()
    {
        return top->data;
        // returns the top value on the stack
    }
    bool isEmpty()
    {
        if (top == nullptr) {
            return true;
        }
        return false;
    }
    int32_t depth()
    {
        return count;
    }
    int32_t snapshot_into(T out[], int32_t maxLen)
    {
        Node* n = top;
        int32_t i = 0;
        while (n != nullptr && i < maxLen) {
            out[i] = n->data;
            n = n->next;
            i++;
        }
        return i;
        // copies every frame, top to bottom in the array given as a parameter
        // this is what buildSnapshot() call, returns count written
    }
};


// Timeline : doubly linked list of Snapshots
struct Snapshot; // fwd declaration;
struct TimelineNode
{
    Snapshot* data;
    TimelineNode* next;
    TimelineNode* prev;
};
class Timeline
{
    TimelineNode* head, * tail;
    int32_t stepCount;

public:
    // Implement these functions
    Timeline()
    {
        head = nullptr;
        tail = nullptr;
        stepCount = 0;
    }
    void record(Snapshot* s)
    {
        TimelineNode* n = new TimelineNode;
        n->data = s;
        n->next = nullptr;
        n->prev = tail;
        if (tail == nullptr) {
            head = n;
            tail = n;
        }
        else {
            tail->next = n;
            tail = n;
        }
        stepCount++;
        // add record in the timeline
    }
    TimelineNode* begin()
    {
        return head;
    }
    int32_t getStepCount()
    {
        return stepCount;
    }
};

// Core structs
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
    int32_t returnLine;
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
void writeHeader(FILE* f, const TTDBHeader& h)
{
    fwrite(h.magic, 1, 4, f);
    fwrite(&h.version, sizeof(int32_t), 1, f);

    // placeholder for other two data members
}

// resolve.bin - bookkeeping
struct FuncEntry
{
    string funcName;
    int64_t byteOffsetInResolveBin; // where this function's FUNC header record sits
};
struct PendingPatch
{
    int64_t byteOffsetOfOffsetField; // where in resolve.bin to seek back and overwrite
    string targetFuncName;
};



// PASS 0x0: READING source.bin + VALIDITY CHECK
bool readSourceLine(ifstream& in, string& out)
{
    while (getline(in, out)) {
        if (!out.empty()) {
            return true;

        }
    }
    return false;
    // reads the next nonblank line
}
string firstWord(const string& line)
{
    size_t idx = line.find(' ');
    if (idx == string::npos) {
        return line;
    }
    return line.substr(0, idx);
    // returns first word from the input string
}
string secondWord(const string& line)
{
    size_t idx1 = line.find(' ');
    if (idx1 == string::npos) {
        return "";
    }
    size_t idx2 = line.find(' ', idx1 + 1);
    if (idx2 == string::npos) {
        return line.substr(idx1 + 1);
    }
    return line.substr(idx1 + 1, idx2 - idx1 - 1);



}
bool validateProgram(const char* sourcePath)
{
    ifstream fin;
    fin.open(sourcePath, ios::binary);
    if (!fin) {
        return false;
    }
    string line;
    Stack<string> validity_stack;


    while (readSourceLine(fin, line)) {
        string word1 = firstWord(line);
        if (word1 == "func") {
            if (!validity_stack.isEmpty()) {
                return false;

            }
            string word2 = secondWord(line);
            validity_stack.push(word2);
        }
        else if (word1 == "func_end") {
            if (validity_stack.isEmpty()) {
                return false;
            }
            validity_stack.pop();
        }
    }
    if (!validity_stack.isEmpty()) {
        return false;
    }
    return true;
    // for each func defined there should be exactly one func_end and no nested funcs allowed - 
}

// PASS 0x1: RESOLVE() -> resolve.bin
int64_t writeResolveRecord(FILE* f, int64_t offsetField, const string& text)
{
    int32_t stringSize = text.size();
    fwrite(&offsetField, sizeof(int64_t), 1, f);
    fwrite(&stringSize, sizeof(int32_t), 1, f);
    fwrite(text.data(), 1, stringSize, f);
    return offsetField;
    // writes one [offset(8B)][size(4B)][string] record at the current file position
    // returns this record's own starting byte position
}
int64_t readResolveRecord(FILE* f, string& outText)
{
    int64_t offsetField;
    int32_t stringSize;
    fread(&offsetField, sizeof(int64_t), 1, f);
    fread(&stringSize, sizeof(int32_t), 1, f);
    char* temp = new char[stringSize];
    fread(temp, 1, stringSize, f);
    outText.assign(temp, stringSize);//using assign bcz our temp doesnt have null terminator
    delete temp;
    return offsetField;
    // reads one record at the current position and advances past it, returns the offset field - the raw line text comes back untouched in outText.
}
int64_t resolveProgram(const char* sourcePath, const char* resolveBinPath)
{
    FuncEntry funcArray[MAX_FUNCS];
    int32_t funcCount = 0;
    PendingPatch patches[MAX_PATCHES];
    int32_t patchCount = 0;
    ifstream fin;
    fin.open(sourcePath, ios::binary);

    if (!fin)
    {
        return -1;
    }
    FILE* fout = fopen(resolveBinPath, "wb");// w=write and b=binary
    if (fout == nullptr) {
        return -1;
    }
    int64_t currentOffset = 0;
    string line;
    int64_t mainOffset = -1;
    while (readSourceLine(fin, line)) {
        writeResolveRecord(fout, currentOffset, line);
        string word1 = firstWord(line);
        if (word1 == "func") {
            string word2 = secondWord(line);
            if (word2 == "main") {
                mainOffset = currentOffset;
            }
            funcArray[funcCount].funcName = word2;
            funcArray[funcCount].byteOffsetInResolveBin = currentOffset;
            funcCount++;


        }
        else if (word1 == "call") {
            patches[patchCount].targetFuncName = secondWord(line);
            patches[patchCount].byteOffsetOfOffsetField = currentOffset;
            patchCount++;
        }
        currentOffset = currentOffset + 8 + 4 + line.size();

    }

    for (int i = 0; i < patchCount; i++) {
        int64_t target_offset = -1;
        for (int j = 0; j < funcCount; j++) {
            if (funcArray[j].funcName == patches[i].targetFuncName) {
                target_offset = funcArray[j].byteOffsetInResolveBin;
                fseek(fout, patches[i].byteOffsetOfOffsetField, SEEK_SET);
                fwrite(&funcArray[j].byteOffsetInResolveBin, sizeof(int64_t), 1, fout);
                break;
            }
        }
        if (target_offset == -1) {
            fclose(fout);
            fin.close();
            return -1;
        }

    }
    if (mainOffset == -1) {
        fin.close();
        fclose(fout);
        return -1;
    }
    fin.close();
    fclose(fout);

    return mainOffset;
    // Every source line becomes one record holding the raw line, as-is.
    // resolve() only PEEKS at the leading word(s) -- enough to spot FUNC
    // (remember its position) and CALL (remember which function it needs
    // and where its offset field sits).
    // Once the whole file is written, every CALL's offset field is patched
    // with its target's position. Patching happens after the full write
    // Returns the byte offset of main's FUNC header record.
    // if there is no main return the error 

}

// PASS 0x2: EXECUTION (tokenization happens here)
enum TokenType
{
    KEYWORD,
    IDENTIFIER,
    PARAM
};
struct Token
{
    TokenType type;
    string text;
};
int32_t tokenizeLine(const string& line, Token tokens[], int32_t maxTokens)
{
    // first word is always a instruction keyword
    // instruction set = [func, func_end, call, set, add, sub, mul and div]
    // next word is identifier like name of a function, variable name
    // after identifier all are the params/arg, space separated
}
Snapshot* buildSnapshot(Stack<Frame>& callStack)
{
    // build the snapshot based on the callStack given
}
void executeProgram(const char* resolveBinPath, int64_t mainOffset, Timeline& timeline)
{
    // initialize the call stack
    // make the main frame
    // push main frame on the call stack

    // implementation:
    // execute line by line, and according to the keyword perform action
}

// PASS 0x3: SERIALIZE TIMELINE
void writeTdbg(Timeline& timeline, const char* tdbgPath)
{
    // placeholder for header
    // index array of the size of stepcount from the timeline
    // placing each snapshot in the file while maintaining the index(starting point of each nth snapshot)
    // after timeline add the index array i the file
    // update the header
}
// main section
int32_t main()
{

    if (!validateProgram("source.bin"))
    {
        // send an error response instead of a .tdbg file
        return 1;
    }

    int64_t mainOffset = resolveProgram("source.bin", "resolve.bin");

    Timeline timeline;
    executeProgram("resolve.bin", mainOffset, timeline);

    writeTdbg(timeline, "session.tdbg");

    return 0;
}
