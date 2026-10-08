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
struct CallInfo
{
    int64_t returnOffset;
    string argSource[MAX_VARS_PER_FRAME];
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
    delete[] temp;
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
    int64_t currentOffset = 0;//of resolvebin
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
    int32_t token_count = 0;
    if (token_count < maxTokens) {

        tokens[token_count].text = firstWord(line);
        tokens[token_count].type = KEYWORD;
        token_count++;
    }
    size_t firstSpace = line.find(' ');
    if (firstSpace == string::npos) {
        return token_count;
    }
    if (token_count < maxTokens) {
        tokens[token_count].text = secondWord(line);
        tokens[token_count].type = IDENTIFIER;
        token_count++;
    }
   
    size_t secondSpace = line.find(' ', firstSpace + 1);
    if (secondSpace == string::npos) {
        return token_count;
    }
    size_t start = secondSpace + 1;
    while (start < line.length() && token_count < maxTokens) {
        size_t end = line.find(' ', start);
        if (end == string::npos) {
            end = line.length();
        }
        tokens[token_count].type = PARAM;
        tokens[token_count].text = line.substr(start, end - start);
        token_count++;
        start = end + 1;
    }
    return token_count;
    // first word is always a instruction keyword
    // instruction set = [func, func_end, call, set, add, sub, mul and div]
    // next word is identifier like name of a function, variable name
    // after identifier all are the params/arg, space separated
}
Snapshot* buildSnapshot(Stack<Frame>& callStack)
{
    Snapshot* snapshot = new Snapshot;
    snapshot->stackDepth = callStack.snapshot_into(snapshot->callStack,MAX_STACK_DEPTH);
    return snapshot;
    // build the snapshot based on the callStack given
}
Variable* findVariable(Frame& frame, const string& name) {
    for (int32_t i = 0; i < frame.argc; i++) {
        if (frame.argv[i].name == name) {
            return &frame.argv[i];
        }
    }
    for (int32_t i = 0; i < frame.localCount; i++) {
        if (frame.locals[i].name == name) {
            return &frame.locals[i];
        }
    }
    return nullptr;
}
void executeProgram(const char* resolveBinPath, int64_t mainOffset, Timeline& timeline)
{
    FILE* fin = fopen(resolveBinPath, "rb");
    if (fin == nullptr) {
        return;
    }
    Stack<Frame> callStack;
    Stack<CallInfo> callinfo;
    Frame main_frame;
    main_frame.func_name = "main";
    main_frame.argc = 0;
    main_frame.returnLine = -1;
    main_frame.localCount = 0;
    callStack.push(main_frame);
    CallInfo main_info;
    main_info.returnOffset = -1;
    callinfo.push(main_info);
    fseek(fin, mainOffset, SEEK_SET);
    while (!callStack.isEmpty()) {
        int64_t currentOffset = ftell(fin);
        string line;
        int64_t targetOffset = readResolveRecord(fin, line);
        int64_t nextOffset = ftell(fin);
        Token tokens[MAX_TOKENS];
        int32_t tokenCount = tokenizeLine(line, tokens, MAX_TOKENS);
        if (tokenCount == 0) {
            break;
        }
        Frame& currentFrame = callStack.peek();
        if (tokens[0].text == "func") {

        }
        else if (tokens[0].text == "set") {
            string var_name = tokens[1].text;
            int32_t value = stoi(tokens[2].text);
            Variable* variable = findVariable(currentFrame, var_name);
            if (variable != nullptr) {
                variable->value = value;
            }
            else if (currentFrame.localCount < MAX_VARS_PER_FRAME) {
                currentFrame.locals[currentFrame.localCount].name = var_name;
                currentFrame.locals[currentFrame.localCount].value = value;
                currentFrame.localCount++;
            }
        }
        else if (tokens[0].text == "add") {
            string a = tokens[1].text;
            string b = tokens[2].text;
            Variable* a1 = findVariable(currentFrame, a);
            Variable* b1 = findVariable(currentFrame, b);
            if (a1 != nullptr && b1 != nullptr) {
                a1->value = a1->value + b1->value;
            }

        }
        else if (tokens[0].text == "sub") {
            string a = tokens[1].text;
            string b = tokens[2].text;
            Variable* a1 = findVariable(currentFrame, a);
            Variable* b1 = findVariable(currentFrame, b);
            if (a1 != nullptr && b1 != nullptr) {
                a1->value = a1->value - b1->value;
            }

        }
        else if (tokens[0].text == "mul") {
            string a = tokens[1].text;
            string b = tokens[2].text;
            Variable* a1 = findVariable(currentFrame, a);
            Variable* b1 = findVariable(currentFrame, b);
            if (a1 != nullptr && b1 != nullptr) {
                a1->value = a1->value * b1->value;
            }

        }
        else if (tokens[0].text == "div") {
            string a = tokens[1].text;
            string b = tokens[2].text;
            Variable* a1 = findVariable(currentFrame, a);
            Variable* b1 = findVariable(currentFrame, b);
            if (a1 != nullptr && b1 != nullptr) {
                a1->value = a1->value / b1->value;
            }

        }
        
        else if (tokens[0].text == "call") {
            int64_t returnOffset = nextOffset;
            fseek(fin, targetOffset, SEEK_SET);
            string funcline;
            readResolveRecord(fin, funcline);
            Token func_tokens[MAX_TOKENS];
            int32_t funcTokenCount = tokenizeLine(funcline, func_tokens, MAX_TOKENS);
            Frame new_frame;
            new_frame.func_name = func_tokens[1].text;
            new_frame.argc = 0;
            new_frame.returnLine = returnOffset;
            new_frame.localCount = 0;
            CallInfo newinfo;
            newinfo.returnOffset = returnOffset;
            for (int32_t i = 2; i < funcTokenCount && new_frame.argc < MAX_VARS_PER_FRAME; i++) {
                int32_t argIndex = i;
                if (argIndex >= tokenCount) {
                    break;
                }
                string paraName = func_tokens[i].text;
                string argName = tokens[argIndex].text;
                new_frame.argv[new_frame.argc].name = paraName;
                newinfo.argSource[new_frame.argc] = argName;

                Variable* argument = findVariable(currentFrame, argName);
                if (argument != nullptr) {
                    new_frame.argv[new_frame.argc].value = argument->value;
                }
                else {

                    new_frame.argv[new_frame.argc].value = 0;
                }
                new_frame.argc++;
            }
            callStack.push(new_frame);
            callinfo.push(newinfo);
        }
        else if (tokens[0].text == "func_end") {
            Frame finishedFrame = callStack.pop();
            CallInfo finishedInfo = callinfo.pop();
            if (!callStack.isEmpty()) {
                Frame& callerFrame = callStack.peek();
                for (int32_t i = 0; i < finishedFrame.argc; i++) {
                    string sourceName = finishedInfo.argSource[i];
                    Variable* callerVariable = findVariable(callerFrame, sourceName);
                    if (callerVariable != nullptr) {
                        callerVariable->value = finishedFrame.argv[i].value;
                    }
                }
            }
            fseek(fin, finishedInfo.returnOffset, SEEK_SET);
        }
        Snapshot* snapshot = buildSnapshot(callStack);
        timeline.record(snapshot);
        fseek(fin, nextOffset, SEEK_SET);
    }

    fclose(fin);


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
    if (mainOffset == -1) {
        return 1;
    }
    Timeline timeline;
    executeProgram("resolve.bin", mainOffset, timeline);

    writeTdbg(timeline, "session.tdbg");

    return 0;
}
