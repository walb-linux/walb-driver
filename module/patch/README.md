## (0) Assumption

- Assume WORK working directory of walb module git repository.
- Assume KERN_SRC linux kernel source tree.
- Assume PATCH_VER patch version like '4.4-ubuntu'.

## (1) mkdir

```
mkdir -p KERN_SRC/include/linux/walb
mkdir -p KERN_SRC/drivers/block/walb
```

## (2) copy files

```
cp WORK/include/walb/*.h KERN_SRC/include/linux/walb/
cp WORK/module/*.{h,c} KERN_SRC/drivers/block/walb/
cp WORK/module/patch/PATCH_VER/patch/Makefile KERN_SRC/drivers/block/walb/
cp WORK/module/patch/PATCH_VER/Kconfig KERN_SRC/drivers/block/walb/
```

## (3) patch files in the parent directory.

```
cd KERN_SRC
patch -p1 < WORK/module/patch/PATCH_VER/Makefile.patch
patch -p1 < WORK/module/patch/PATCH_VER/Kconfig.patch
```
