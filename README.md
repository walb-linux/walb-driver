# Description

WalB is a block device driver for linux kernel that stores write-ahead logs additionally for efficient backup and replication.

# License

GPL version 2 or 3.

# Directories

* include: header files shared by kernel/userland code.
* module: linux device driver source files.
* doc: documents.
* tool(deprecated): userland tools to control walb devices.
* sim(deprecated): simulators to check algorithm consistency.

# Supported kernel version.

Use an appropriate branch or a tag for your using kernel as follows:

| Branch     | Tag      | Kernel version |
|------------|----------|----------------|
| `master`   | `v1.2.x` | 3.14-          |
| `for-3.13` | `v1.1.x` | 3.10-3.13      |
| `for-3.2`  | `v1.0.x` | 3.2-3.8        |

for-3.2 branch is not maintained anymore.
The following fixes have not applied:

- bugfix: forget to check existance of device with the same name.
- bugfix: it must wait for checkpointing to keep consistency.
- bugfix: forget to check overflow at device creation.
- crash-test-related bugfixes
  - bugfix: forget to check overflow at device creation.
  - bugfix: call iocore_flush() before put_disk().
  - bugfix: split destroy_wdev() into finalize_wdev() and destroy_wdev() and the latter must wait for n_users to be 0.
  - bugfix: set overflow flag wrongly when log space is empty.
- bugfix: bio contents may change during IO execution.
