from basic_defs import cloud_storage, NAS

import hashlib
import boto3
from google.cloud import storage
from azure.storage.blob import BlobServiceClient

blocksize = cloud_storage.block_size
class AWS_S3(cloud_storage):
    def __init__(self):
        # TODO: Fill in the AWS access key ID
        self.access_key_id = ""
        # TODO: Fill in the AWS access secret key
        self.access_secret_key = ""
        # TODO: Fill in the bucket name
        self.bucket_name = ""
        self.s3 = boto3.resource('s3', aws_access_key_id=self.access_key_id, aws_secret_access_key=self.access_secret_key)
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.s3_client = boto3.client('s3', aws_access_key_id=self.access_key_id, aws_secret_access_key=self.access_secret_key)
    # Implement the abstract functions from cloud_storage
    def list_blocks(self):
        block_list = []
        blocks = self.bucket.objects.all()
        for block in blocks:
            block_list.append(int(block.key))
        return block_list

    def read_block(self, offset):
        try:
            object = self.s3.Object(self.bucket_name, str(offset))
            response = object.get()
            return bytearray(response['Body'].read())
        except:
            return bytearray("")

    def write_block(self, block, offset):
        block = str(block)
        self.bucket.put_object(Key=str(offset), Body=block)

    def delete_block(self, offset):
        self.s3_client.delete_object(Bucket=self.bucket_name, Key=str(offset))

class Azure_Blob_Storage(cloud_storage):
    def __init__(self):
        # TODO: Fill in the Azure key
        self.key = ""
        # TODO: Fill in the Azure connection string
        self.conn_str = ""
        # TODO: Fill in the account name
        self.account_name = ""
        # TODO: Fill in the container name
        self.container_name = ""
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)

    # Implement the abstract functions from cloud_storage
    def list_blocks(self):
        blobs = self.blob_service_client.get_container_client(self.container_name).list_blobs()
        block_list = []
        for blob in blobs:
            block_list.append(int(blob.name))
        return block_list

    def read_block(self, offset):
        blob_client = self.blob_service_client.get_blob_client(self.container_name,str(offset))
        if blob_client.exists():
            output = blob_client.download_blob()
            return bytearray(output.readall())
        else:
            return bytearray("")

    def write_block(self, block, offset):
        block = str(block)
        blob_client = self.blob_service_client.get_blob_client(self.container_name,str(offset))
        if blob_client.exists():
            blob_client.delete_blob()
        blob_client.upload_blob(block, blob_type="BlockBlob")

    def delete_block(self, offset):
        blob_client = self.blob_service_client.get_blob_client(self.container_name,str(offset))
        if blob_client.exists():
            blob_client.delete_blob()
            return 1
        else:
            return 0

class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        # Google Cloud Storage is authenticated with a **Service Account**
        # TODO: Download and place the Credential JSON file
        self.credential_file = ""
        # TODO: Fill in the container name
        self.bucket_name = ""
        self.storage_client = storage.Client.from_service_account_json(self.credential_file)
        self.bucket = self.storage_client.get_bucket(self.bucket_name)

    def list_blocks(self):
        blobs = self.storage_client.list_blobs(self.bucket_name)
        blocks_list = []
        for blob in blobs:
            blocks_list.append(int(blob.name))
        return blocks_list

    def read_block(self, offset):
        blob = self.bucket.get_blob(str(offset))
        if blob != None:
            output = blob.download_as_string()
            return bytearray(output)
        else:
            return bytearray("")

    def write_block(self, block, offset):
        block = str(block)
        blob = self.bucket.get_blob(str(offset))
        if(blob != None):
            self.bucket.delete_blob(str(offset))
        new_blob = self.bucket.blob(str(offset))
        new_blob.upload_from_string(block)
        return "Data Write Successful"

    def delete_block(self, offset):
        blob = self.bucket.get_blob(str(offset))
        if(blob != None):
            self.bucket.delete_blob(str(offset))
            return 1
        else:
            return 0

class RAID_on_Cloud(NAS):
    def __init__(self):
        self.backends = [
                AWS_S3(),
                Azure_Blob_Storage(),
                Google_Cloud_Storage()
            ]
        self.fds = dict()

    def open(self, filename):
        newfd = None
        for fd in range(256):
            if fd not in self.fds:
                newfd = fd
                break
        if newfd is None:
            raise IOError("Opened files exceed system limitation.")
        self.fds[newfd] = filename
        return newfd

    def read(self, fd, l, offset):
        if fd not in self.fds:
            return ""
        filename = self.fds[fd]
        key = self.hashGen(filename)
        output1 = ""
        output2 = ""
        mapping = self.backendSelector(key)
        backend1 = self.backends[mapping[0]]
        backend2 = self.backends[mapping[1]]

        start = offset/blocksize
        end = (l+offset)/blocksize
        if((l+offset)%blocksize == 0):
            end = end - 1
        key = key + start
        while start <= end:
            output1 = output1 + str(backend1.read_block(key))
            output2 = output2 + str(backend2.read_block(key))
            start = start+1
            key = key+1
        offset = offset % blocksize
        output = ""
        if(output1 != ""):
            output = output1
        else:
            output = output2

        if(len(output)>=l+offset):
            output = output[offset:l+offset]
        else:
            if(offset>len(output)):
                output = ""
            elif(len(output) > offset and len(output) < l + offset):
                output = output[offset:]
        return output#.strip('\0')

    def write(self, fd, data, offset):
        if fd not in self.fds:
            raise IOError("File descriptor %d does not exist." % fd)
        filename = self.fds[fd]
        key = self.hashGen(filename)
        mapping = self.backendSelector(key)

        backend1 = self.backends[mapping[0]]
        backend2 = self.backends[mapping[1]]

        key = key + offset/blocksize
        offset = offset % blocksize
        block_offset = (len(data)+offset)/blocksize
        while(block_offset >= 0):
            output = str(backend1.read_block(key))
            if(output != ""):#block exists and contains existing data
                if(len(output) > offset):
                    temp = output[offset:]
                    if(len(temp)<= len(data)):
                        if(len(data)+offset<=blocksize):
                            output = output[:offset] + data
                            offset = 0
                            backend1.write_block(bytearray(output), key)
                            backend2.write_block(bytearray(output), key)
                        else:
                            output = output[:offset] + data[:blocksize - offset]
                            data = data[blocksize - offset:]
                            offset = 0
                            backend1.write_block(bytearray(output), key)
                            backend2.write_block(bytearray(output), key)
                            key = key + 1
                    else:
                        output = output[:offset] + data + output[offset + len(data):]
                        offset = 0
                        backend1.write_block(bytearray(output), key)
                        backend2.write_block(bytearray(output), key)
                else:
                    default_str = ""
                    default_str = default_str + "\0" * (offset-len(output))
                    if (len(data) + offset <= blocksize):
                        output = output + default_str + data
                        offset = 0
                        backend1.write_block(bytearray(output), key)
                        backend2.write_block(bytearray(output), key)
                    else:
                        output = output + default_str + data[:blocksize - offset]
                        data = data[blocksize - offset:]
                        offset = 0
                        backend1.write_block(bytearray(output), key)
                        backend2.write_block(bytearray(output), key)
                        key = key + 1
            else: #block does not contain data
                default_str = ""
                default_str = default_str + "\0" * offset
                if len(data) + offset > blocksize:
                    temp = default_str + data[:blocksize-offset]
                    backend1.write_block(bytearray(temp), key)
                    backend2.write_block(bytearray(temp), key)
                    data = data[blocksize-offset:]
                    key = key + 1
                    offset = 0
                else:
                    temp = default_str + data
                    backend1.write_block(bytearray(temp), key)
                    backend2.write_block(bytearray(temp), key)
                    offset = 0
            block_offset = block_offset - 1

    def close(self, fd):
        if fd not in self.fds:
            raise IOError("File descriptor %d does not exist." % fd)
        del self.fds[fd]
        return

    def delete(self, filename):
        key = self.hashGen(filename)
        mapping = self.backendSelector(key)
        i=0
        while(i<len(mapping)):
            backend = self.backends[mapping[i]]
            if(mapping[i]==0): #backend is AWS_S3
                temp = key
                backend.delete_block(str(temp))
                try:
                    while(True):
                        self.s3_client.head_object(Bucket=self.backends[0].bucket_name,Key=str(temp))
                        backend.delete_block(str(temp))
                        temp=temp+1
                except:
                    i = i + 1
                    continue
            else:
                output = 1
                temp = key
                while(output!=0):
                    output = backend.delete_block(str(temp))
                    temp = temp + 1
            i = i + 1

    def hashGen(self, file_name):
        return int(hashlib.md5(file_name).hexdigest(), 16) % 100000000

    def backendSelector(self, hash):
        #Mapper to find out which cloud storages have our file
        # 0 -> AWS, 1 -> Azure, 2 -> GCP
        ret_val = []
        case = hash % 3
        if(case == 0):
            ret_val = [0,1]
        elif(case == 1):
            ret_val = [1,2]
        else:
            ret_val = [2,0]
        return ret_val