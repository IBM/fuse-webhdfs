from six.moves import http_client
import re

import requests
try:
    from urllib.parse import quote, quote_plus
except ImportError:
    from urllib import quote, quote_plus

from pywebhdfs import errors, operations


class PyWebHdfsClient(object):
    """
    PyWebHdfsClient is a Python wrapper for the Hadoop WebHDFS REST API

    To use this client:

    >>> from pywebhdfs.webhdfs import PyWebHdfsClient
    """

    def __init__(self, host='localhost', port='50070', user_name=None,
                 path_to_hosts=None, timeout=120,
                 base_uri_pattern="http://{host}:{port}/webhdfs/v1/",
                 request_extra_opts={}):
        """
        Create a new client for interacting with WebHDFS

        :param host: the ip address or hostname of the HDFS namenode
        :param port: the port number for WebHDFS on the namenode
        :param user_name: WebHDFS user.name used for authentication
        :param path_to_hosts: mapping paths to hostnames for federation
        :param timeout: timeout for the underlying HTTP request (def: 120 sec)
        :param base_uri_pattern: format string for base URI
        :param request_extra_opts: dictionary of extra options to pass
          to the requests library (e.g., SSL, HTTP authentication, etc.)

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')

        Via a secure Knox gateway:

        >>> hdfs = PyWebHdfsClient(base_uri_pattern=
        >>>     "https://knox.mycluster.local:9443/gateway/default/webhdfs/v1",
        >>>     request_extra_opts={'verify': '/etc/ssl/myrootca.crt',
        >>>                         'auth': ('username', 'password')})
        """

        self.host = host
        self.port = port
        self.user_name = user_name
        self.timeout = timeout
        self.session = requests.Session()
        self.path_to_hosts = path_to_hosts
        if self.path_to_hosts is None:
            self.path_to_hosts = [('.*', [self.host])]

        self.base_uri_pattern = base_uri_pattern.format(
            host="{host}", port=port)
        self.request_extra_opts = request_extra_opts

    def create_file(self, path, file_data, **kwargs):
        """
        Creates a new file on HDFS

        :param path: the HDFS file path
        :param file_data: the initial data to write to the new file

        The function wraps the WebHDFS REST call:

        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE

        [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
        [&permission=<OCTAL>][&buffersize=<INT>]

        The function accepts all WebHDFS optional arguments shown above

        Example:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_data = '01010101010101010101010101010101'
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> hdfs.create_file(my_file, my_data)

        Example with optional args:

        >>> hdfs.create_file(my_file, my_data, overwrite=True, blocksize=64)

        Or for sending data from file like objects:

        >>> with open('file.data') as file_data:
        >>>     hdfs.create_file(hdfs_path, data=file_data)


        Note: The create_file function does not follow automatic redirects but
        instead uses a two step call to the API as required in the
        WebHDFS documentation
        """

        # make the initial CREATE call to the HDFS namenode
        optional_args = kwargs

        init_response = self._resolve_host(self.session.put, False,
                                           path, operations.CREATE,
                                           **optional_args)
        if not init_response.status_code == http_client.TEMPORARY_REDIRECT:
            _raise_pywebhdfs_exception(
                init_response.status_code, init_response.content)

        # Get the address provided in the location header of the
        # initial response from the namenode and make the CREATE request
        # to the datanode
        uri = init_response.headers['location']
        response = self.session.put(
            uri, data=file_data,
            headers={'content-type': 'application/octet-stream'},
            **self.request_extra_opts)

        if not response.status_code == http_client.CREATED:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return True

    def append_file(self, path, file_data, **kwargs):
        """
        Appends to an existing file on HDFS

        :param path: the HDFS file path
        :param file_data: data to append to existing file

        The function wraps the WebHDFS REST call:

        POST http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND

        [&buffersize=<INT>]

        The function accepts all WebHDFS optional arguments shown above

        Example:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_data = '01010101010101010101010101010101'
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> hdfs.append_file(my_file, my_data)

        Example with optional args:

        >>> hdfs.append_file(my_file, my_data, overwrite=True, buffersize=4096)

        Note: The append_file function does not follow automatic redirects but
        instead uses a two step call to the API as required in the
        WebHDFS documentation

        Append is not supported in Hadoop 1.x
        """

        # make the initial APPEND call to the HDFS namenode
        optional_args = kwargs

        init_response = self._resolve_host(self.session.post, False,
                                           path, operations.APPEND,
                                           **optional_args)
        if not init_response.status_code == http_client.TEMPORARY_REDIRECT:
            _raise_pywebhdfs_exception(
                init_response.status_code, init_response.content)

        # Get the address provided in the location header of the
        # initial response from the namenode and make the APPEND request
        # to the datanode
        uri = init_response.headers['location']
        response = self.session.post(
            uri, data=file_data,
            headers={'content-type': 'application/octet-stream'},
            **self.request_extra_opts
        )

        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return True

    def read_file(self, path, **kwargs):
        """
        Reads from a file on HDFS  and returns the content

        :param path: the HDFS file path

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN

        [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]

        Note: this function follows automatic redirects

        Example:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> hdfs.read_file(my_file)
        01010101010101010101010101010101
        01010101010101010101010101010101
        01010101010101010101010101010101
        01010101010101010101010101010101
        """

        optional_args = kwargs

        response = self._resolve_host(self.session.get, True,
                                      path, operations.OPEN,
                                      **optional_args)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return response.content

    def stream_file(self, path, chunk_size=1024, **kwargs):
        """
        Reads from a file on HDFS  and returns the content

        :param path: the HDFS file path

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN

        [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]

        Note: this function follows automatic redirects

        Example:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> hdfs.read_file(my_file)
        01010101010101010101010101010101
        01010101010101010101010101010101
        01010101010101010101010101010101
        01010101010101010101010101010101
        """

        optional_args = kwargs

        response = self._resolve_host(self.session.get, True,
                                      path, operations.OPEN, stream=True,
                                      **optional_args)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        for chunk in response.iter_content(chunk_size):
            if chunk:
                yield chunk

    def make_dir(self, path, **kwargs):
        """
        Create a new directory on HDFS

        :param path: the HDFS file path

        The function wraps the WebHDFS REST call:

        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS

        [&permission=<OCTAL>]

        Example:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_dir = 'user/hdfs/data/new_dir'
        >>> hdfs.make_dir(my_dir)

        Example with optional args:

        >>> hdfs.make_dir(my_dir, permission=755)
        """

        optional_args = kwargs

        response = self._resolve_host(self.session.put, True,
                                      path, operations.MKDIRS,
                                      **optional_args)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return True

    def rename_file_dir(self, path, destination_path):
        """
        Rename an existing directory or file on HDFS

        :param path: the HDFS file path
        :param destination_path: the new file path name

        The function wraps the WebHDFS REST call:

        PUT <HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>

        Example:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> current_dir = 'user/hdfs/data/my_dir'
        >>> destination_dir = 'user/hdfs/data/renamed_dir'
        >>> hdfs.rename_file_dir(current_dir, destination_dir)
        """

        destination_path = '/' + destination_path.lstrip('/')

        response = self._resolve_host(self.session.put, True,
                                      path, operations.RENAME,
                                      destination=destination_path)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return response.json()

    def delete_file_dir(self, path, recursive=False):
        """
        Delete an existing file or directory from HDFS

        :param path: the HDFS file path

        The function wraps the WebHDFS REST call:

        DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE

        [&recursive=<true|false>]

        Example for deleting a file:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> hdfs.delete_file_dir(my_file)

        Example for deleting a directory:

        >>> hdfs.delete_file_dir(my_file, recursive=True)
        """

        response = self._resolve_host(self.session.delete, True,
                                      path, operations.DELETE,
                                      recursive=recursive)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return True

    def get_file_dir_status(self, path):
        """
        Get the file_status of a single file or directory on HDFS

        :param path: the HDFS file path

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS

        Example for getting file status:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> hdfs.get_file_dir_status(my_file)
        {
            "FileStatus":{
                "accessTime":1371737704282,
                "blockSize":134217728,
                "group":"hdfs",
                "length":90,
                "modificationTime":1371737704595,
                "owner":"hdfs",
                "pathSuffix":"",
                "permission":"755",
                "replication":3,
                "type":"FILE"
            }
        }

        Example for getting directory status:

        >>> my_dir = 'user/hdfs/data/'
        >>> hdfs.get_file_dir_status(my_file)
        {
            "FileStatus":{
                "accessTime":0,
                "blockSize":0,
                "group":"hdfs",
                "length":0,
                "modificationTime":1371737704208,
                "owner":"hdfs",
                "pathSuffix":"",
                "permission":"755",
                "replication":0,
                "type":"DIRECTORY"
            }
        }
        """

        response = self._resolve_host(self.session.get, True,
                                      path, operations.GETFILESTATUS)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return response.json()

    def get_content_summary(self, path):
        """
        Get the content summary of a directory on HDFS

        :param path: the HDFS file path

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY

        Example for getting a directory's content summary:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_folder = 'user/hdfs/data/'
        >>> hdfs.get_file_dir_status(my_folder)
        {
            "ContentSummary":
            {
                "directoryCount": 2,
                "fileCount": 1,
                "length": 24930,
                "quota": -1,
                "spaceConsumed": 24930,
                "spaceQuota": -1
            }
        }
        """

        response = self._resolve_host(self.session.get, True,
                                      path, operations.GETCONTENTSUMMARY)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return response.json()

    def get_file_checksum(self, path):
        """
        Get the file_checksum of a single file on HDFS

        :param path: the HDFS file path

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM

        Example for getting file status:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> hdfs.get_file_checksum(my_file)
        {
            "FileChecksum":{
                "algorithm": "MD5-of-1MD5-of-512CRC32",
                "bytes": "000002000000000000000000729a144ad5e9399f70c9bed...",
                "length": 28
            }
        }
        """

        response = self._resolve_host(self.session.get, True,
                                      path, operations.GETFILECHECKSUM)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return response.json()

    def list_dir(self, path):
        """
        Get a list of file_status for all files and directories
        inside an HDFS directory

        :param path: the HDFS file path

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS

        Example for listing a directory:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_dir = 'user/hdfs'
        >>> hdfs.list_dir(my_dir)
        {
            "FileStatuses":{
                "FileStatus":[
                    {
                        "accessTime":1371737704282,
                        "blockSize":134217728,
                        "group":"hdfs",
                        "length":90,
                        "modificationTime":1371737704595,
                        "owner":"hdfs",
                        "pathSuffix":"example3.txt",
                        "permission":"755",
                        "replication":3,
                        "type":"FILE"
                    },
                    {
                        "accessTime":1371678467205,
                        "blockSize":134217728,
                        "group":"hdfs","length":1057,
                        "modificationTime":1371678467394,
                        "owner":"hdfs",
                        "pathSuffix":"example2.txt",
                        "permission":"700",
                        "replication":3,
                        "type":"FILE"
                    }
                ]
            }
        }

        """

        response = self._resolve_host(self.session.get, True,
                                      path, operations.LISTSTATUS)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return response.json()

    def exists_file_dir(self, path):
        """
        Checks whether a file or directory exists on HDFS

        :param path: the HDFS file path

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS

        and returns a bool based on the response.

        Example for checking whether a file exists:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> hdfs.exists_file_dir(my_file)
        True
        """
        response = self._resolve_host(self.session.get, True,
                                      path, operations.GETFILESTATUS)
        if response.status_code == http_client.OK:
            return True
        elif response.status_code == http_client.NOT_FOUND:
            return False
        _raise_pywebhdfs_exception(response.status_code, response.content)

    def set_permission(self, path, permission):
        """
        Set permission of a file on HDFS

        :param path: the HDFS file path
        :param permission: the permission of the HDFS file

        The function wraps the WebHDFS REST call:

        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION

        [&permission=<OCTAL>]

        Example:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_dir = 'user/hdfs/data/new_dir'
        >>> hdfs.set_permission(my_dir, 755)
        """

        response = self._resolve_host(self.session.put, False,
                                      path, operations.SETPERMISSION,
                                      permission=permission)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return True

    def set_owner(self, path, owner, group):
        """
        Set owner of a file on HDFS

        :param path: the HDFS file path
        :param owner: the owner of the HDFS file
        :param group: the group of the HDFS file

        The function wraps the WebHDFS REST call:

        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER

        [&owner=<USER>][&group=<GROUP>]

        Example:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> my_dir = 'user/hdfs/data/new_dir'
        >>> hdfs.set_permission(my_dir, 'webuser', 'supergroup')
        """

        response = self._resolve_host(self.session.put, False,
                                      path, operations.SETPERMISSION,
                                      owner=owner, group=group)
        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)

        return True

    def get_xattr(self, path, xattr=None):
        """
        Get extended attributes set on an HDFS path

        :param path: the HDFS file path without a leading '/'
        :param xattr: the extended attribute to get

        This function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETXATTRS

        [&xattr.name=<string>]

        Example for getting an extended attribute:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> hdfs.get_xattr('user/hdfs/data.txt', xattr='user.important')
        {
            "XAttrs": [
                {
                    "name":"user.important",
                    "value":"very"
                }
            ]
        }
        """
        kwd_params = {}
        if xattr:
            kwd_params['xattr.name'] = xattr

        response = self._resolve_host(self.session.get, True,
                                      path, operations.GETXATTRS,
                                      **kwd_params)

        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)
        return response.json()

    def set_xattr(self, path, xattr, value, replace=False):
        """
        Set an extended attribute on an HDFS path

        :param path: the HDFS file path without a leading '/'
        :param xattr: the extended attribute to set
        :param value: the value of the extended attribute
        :param replace: replace the extended attribute

        This function wraps the WebHDFS REST call:

        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETXATTR

        [&xattr.name=<string>][&xattr.value=<string][&flag=<REPLACE|CREATE>]

        Example for setting an extended attribute:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> hdfs.set_xattr('user/hdfs/data.txt', 'user.important', 'very')
        """
        kwd_params = {
            'xattr.name': xattr,
            'xattr.value': value
        }
        if replace:
            kwd_params['flag'] = "REPLACE"
        else:
            kwd_params['flag'] = "CREATE"

        response = self._resolve_host(self.session.put, True,
                                      path, operations.SETXATTR,
                                      **kwd_params)

        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)
        return True

    def list_xattrs(self, path):
        """
        List all the extended attributes set on an HDFS path

        :param path: the HDFS file path without a leading '/'

        This function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTXATTRS

        Example for getting an extended attribute:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> hdfs.list_xattrs('user/hdfs/data.txt')
        {
            "XAttrNames": "[\"XATTRNAME1\",\"XATTRNAME2\",\"XATTRNAME3\"]"
        }
        """
        response = self._resolve_host(self.session.get, True,
                                      path, operations.LISTXATTRS)

        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)
        return response.json()

    def delete_xattr(self, path, xattr):
        """
        Delete the extended attribute set on an HDFS path

        :param path: the HDFS file path without a leading '/'
        :param xattr: the extended attribute to delete

        This function wraps the WebHDFS REST call:

        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEXATTR

        [&xattr.name=<string>]

        Example for getting an extended attribute:

        >>> hdfs = PyWebHdfsClient(host='host',port='50070', user_name='hdfs')
        >>> hdfs.delete_xattr('user/hdfs/data.txt', 'user.important')
        """
        kwd_params = {
            'xattr.name': xattr
        }

        response = self._resolve_host(self.session.put, True,
                                      path, operations.REMOVEXATTR,
                                      **kwd_params)

        if not response.status_code == http_client.OK:
            _raise_pywebhdfs_exception(response.status_code, response.content)
        return True

    def _create_uri(self, path, operation, **kwargs):
        """
        internal function used to construct the WebHDFS request uri based on
        the <PATH>, <OPERATION>, and any provided optional arguments
        """

        no_root_path = (path[1:] if path[0] == '/' else path)
        path_param = quote(no_root_path.encode('utf8'))

        # setup the parameter represent the WebHDFS operation
        operation_param = '?op={operation}'.format(operation=operation)

        # configure authorization based on provided credentials
        auth_param = str()
        if self.user_name:
            auth_param = '&user.name={user_name}'.format(
                user_name=self.user_name)

        # setup any optional parameters
        keyword_params = str()
        for key in kwargs:
            try:
                value = quote_plus(kwargs[key].encode('utf8'))
            except:
                value = str(kwargs[key]).lower()
            keyword_params = '{params}&{key}={value}'.format(
                params=keyword_params, key=key, value=value)

        base_uri = self.base_uri_pattern.format(host="{host}")

        # build the complete uri from the base uri and all configured params
        uri = '{base_uri}{path}{operation}{keyword_args}{auth}'.format(
            base_uri=base_uri, path=path_param,
            operation=operation_param, keyword_args=keyword_params,
            auth=auth_param)

        return uri

    def _resolve_federation(self, path):
        """
        internal function used to resolve federation
        """
        for path_regexp, hosts in self.path_to_hosts:
            if re.match(path_regexp, path):
                return hosts
        raise errors.CorrespondHostsNotFound(
            msg="Could not find hosts corresponds to /{0}".format(path))

    def _resolve_host(self, req_func, allow_redirect,
                      path, operation, **kwargs):
        """
        internal function used to resolve federation and HA and
        return response of resolved host.
        """
        uri_without_host = self._create_uri(path, operation, **kwargs)
        hosts = self._resolve_federation(path)
        for host in hosts:
            uri = uri_without_host.format(host=host)
            try:
                response = req_func(uri, allow_redirects=allow_redirect,
                                    timeout=self.timeout,
                                    **self.request_extra_opts)

                if not _is_standby_exception(response):
                    _move_active_host_to_head(hosts, host)
                    return response
            except requests.exceptions.RequestException:
                continue
        raise errors.ActiveHostNotFound(msg="Could not find active host")


def _raise_pywebhdfs_exception(resp_code, message=None):

    if resp_code == http_client.BAD_REQUEST:
        raise errors.BadRequest(msg=message)
    elif resp_code == http_client.UNAUTHORIZED:
        raise errors.Unauthorized(msg=message)
    elif resp_code == http_client.NOT_FOUND:
        raise errors.FileNotFound(msg=message)
    elif resp_code == http_client.METHOD_NOT_ALLOWED:
        raise errors.MethodNotAllowed(msg=message)
    else:
        raise errors.PyWebHdfsException(msg=message)


def _is_standby_exception(response):
    """
    check whether response is StandbyException or not.
    """
    if response.status_code == http_client.FORBIDDEN:
        try:
            body = response.json()
            exception = body["RemoteException"]["exception"]
            if exception == "StandbyException":
                return True
        except:
            pass
    return False


def _move_active_host_to_head(hosts, active_host):
    """
    to improve efficiency move active host to head
    """
    hosts.remove(active_host)
    hosts.insert(0, active_host)
