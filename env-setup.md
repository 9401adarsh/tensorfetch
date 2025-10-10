TensorFetch Environment Setup Guide

This guide sets up the complete environment for TensorFetch development inside a GCP VM.

start instance: 
`gcloud compute instances start tensorfetch-vm --zone=us-central1-a`

stop instance:
`gcloud compute instances stop tensorfetch-vm --zone=us-central1-a`

⸻

1. System Update

sudo apt update && sudo apt upgrade -y


⸻

2. Core Build and Tracing Toolchain

sudo apt install -y build-essential clang llvm cmake pkg-config curl jq \
  libelf-dev libbpf-dev zlib1g-dev liburing-dev python3-venv python3-pip


⸻

3. Python and Data Libraries

sudo apt install -y python3-pip python3-venv python3-dev libboost-all-dev

(Optional virtual environment)

python3 -m venv ~/tensorfetch-env
source ~/tensorfetch-env/bin/activate
pip install --upgrade pip
pip install pyarrow pandas fastparquet matplotlib seaborn

Note: Ubuntu ARM repositories don’t include python3-pyarrow — install it via pip as shown above instead of apt.

⸻

4. Enable Kernel Options for eBPF

sudo sysctl -w kernel.unprivileged_bpf_disabled=0
sudo sysctl -w kernel.kptr_restrict=0
echo "kernel.unprivileged_bpf_disabled=0" | sudo tee -a /etc/sysctl.conf
echo "kernel.kptr_restrict=0" | sudo tee -a /etc/sysctl.conf

Fix: Allow eBPF to Lock Memory (Permanent Option)

To allow TensorFetch and bpftrace to create eBPF maps safely, configure permanent memlock limits.
	1.	Edit the limits file:

sudo nano /etc/security/limits.conf

Add:

adarsh9401  hard  memlock  unlimited
adarsh9401  soft  memlock  unlimited


	2.	Ensure PAM loads limits:

sudo nano /etc/pam.d/common-session
sudo nano /etc/pam.d/common-session-noninteractive

Add this line at the end of both files:

session required pam_limits.so


	3.	reboot, then verify:

ulimit -l

Expected output:

unlimited



⸻

5. Verify Toolchain

Check	Command	Expected
Kernel ≥ 5.15	uname -r	e.g., 5.15.x
BPF works	sudo bpftrace -e 'kprobe:__arm64_sys_read { printf("ok\\n"); }'	prints “ok” when you cat /etc/passwd
io_uring works	git clone https://github.com/axboe/liburing.git && cd liburing/examples && make && ./io_uring-cp /etc/hosts /tmp/hosts.copy	copies file successfully
Python/Arrow	python3 -c "import pyarrow; print('OK')"	prints OK


⸻

6. Project Workspace

mkdir -p ~/projects/tensorfetch/{daemon,ebpf,scripts}
cd ~/projects/tensorfetch

⸻

7. Verification Summary
	•	eBPF: working via bpftrace or libbpf program
	•	io_uring: verified async read/write operations
	•	Python: PyArrow + FastParquet installed via pip for data testing
	•	Workspace: TensorFetch folder structure ready for development

⸻