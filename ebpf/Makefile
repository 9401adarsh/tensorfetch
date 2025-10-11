BPF_CLANG = clang
BPF_FLAGS = -O2 -g -target bpf -Wall -Werror

all: openat_probe.bpf.o

openat_probe.bpf.o: openat_probe.bpf.c vmlinux.h
	$(BPF_CLANG) $(BPF_FLAGS) -c $< -o $@
	llvm-strip -g $@

clean:
	rm -f *.o