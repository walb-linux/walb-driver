# Description

WalB is a block device driver for linux kernel that stores write-ahead logs additinally for efficient backup and replication.

# License

GPL version 2 or 3.

# Directories

* include: header files shared by kernel/userland code.
* module: linux device driver source files.
* tool: userland tools to control walb devices.
* sim: simulators to check algorithm consistency (deprecated).
* doc: documents.

# Supported kernel version.

Linux kernel 3.14 to 3.x.
For 3.10 to 3.13, use version 1.1.x.
For 3.2 to 3.8, use version 1.0.x.
