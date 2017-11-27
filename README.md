# Description

Mount WebHDFS on your local Linux or Mac file system

After this, you can access the WebHDFS file system as if was a local directory - with regular Unix file operations.

# Installation

```
pip3 install -r requirements.txt
```

# Usage

In one terminal type:

```
mkdir -p ~/fuse-webhdfs
python3 mount-webhdfs.py ~/fuse-webhdfs
```
You will have to type in your username and password.
To avoid having to type it every time, you have two options:

1) add an entry in the $HOME/.netrc file:
```
machine webhdfs.fully.qualified.domain.name.ibm.com login <username> password <password>
```
or

2) set the following environment variables beforehand:

```
export HDFS_USERNAME=<your username>
export HDFS_PASSWORD=<your password>
```
If you have both the .netrc file and the environment variables, then environment variables will override the .netrc settings.

After mounting, in other terminal(s) you will be able to list files, read them, etc.
For example:

```
ls -l ~/fuse-webhdfs/tmp
echo "this is a test" > ~/fuse-webhdfs/tmp/test
cat ~/fuse-webhdfs/tmp/test
```

