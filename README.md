# Description

Mount WebHDFS on your local Linux or Mac file system

After this, you can access the WebHDFS file system as if was a local directory - with regular Unix file operations.

# Installation

First dependency is fuse, you can install it on Ubuntu with:
```
sudo apt-get install fuse
```

or on RedHat with:
```
sudo yum install fuse
```

```
pip3 install -r requirements.txt
```

# Usage

In one terminal type:

```
mkdir -p ~/fuse-webhdfs
python3 mount-webhdfs.py ~/fuse-webhdfs
```
You now have to type in your HDFS endpoint parameters and HDFS (Knox) username and password.
These parameters will be saved in plain text in `$HOME/.config/webhdfs.ini`.
If you have a problem with that, please create a pull request and I will be happy to consider merging it.


After mounting, in other terminal(s) you will be able to list files, read them, etc.
For example:

```
ls -l ~/fuse-webhdfs/tmp
echo "this is a test" > ~/fuse-webhdfs/tmp/test
cat ~/fuse-webhdfs/tmp/test
```

