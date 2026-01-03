# === Compiler & Flags ===
CXX := clang++
CXXFLAGS := -std=c++20 -Wall -Wextra -O2 -g

# === Includes ===
INCLUDES := -I../ebpf -I../ebpf/include -I/usr/include -I/usr/local/include -Iinclude \
			$(shell pkg-config --cflags arrow parquet)

# === Libraries ===
LIBS := -lbpf -lelf -lz -lpthread -luring \
        $(shell pkg-config --libs arrow parquet)

# === Paths ===
SRC_DIR := src
OBJ_DIR := build
BIN_DIR := bin
TARGET := $(BIN_DIR)/tensorfetchd

# === Sources & Objects ===
SRC := $(SRC_DIR)/main.cpp
OBJ := $(OBJ_DIR)/main.o

# === Default Rule ===
all: $(TARGET)

# === Build Binary ===
$(TARGET): $(OBJ)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(OBJ) -o $(TARGET) $(LIBS)

# === Build Object Files ===
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# === Clean ===
clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR)

.PHONY: all clean