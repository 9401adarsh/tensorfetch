BPF_CLANG = clang
BPF_CFLAGS = -O2 -g -target bpf -D__TARGET_ARCH_x86
USER_CLANG = clang
USER_CFLAGS = -O2 -g -Wall -I/usr/include -I/usr/local/include
LIBS = -lbpf -lelf -lz -lpthread

# Paths
INCLUDES = -Iinclude

all: trace_open.skel.h

trace_open.bpf.o: trace_open.bpf.c include/vmlinux.h include/event.h
	$(BPF_CLANG) $(BPF_CFLAGS) $(INCLUDES) -c $< -o $@

trace_open.skel.h: trace_open.bpf.o
	bpftool gen skeleton $< > $@

clean:
	rm -f *.o *.skel.h

# --- Build the userspace sanity loader ---
main: main.c trace_open.skel.h
	$(USER_CLANG) $(USER_CFLAGS) $< -o $@ $(LIBS)

.PHONY: all clean main