import boto3
import os


def compare(objkey, key):
    if isinstance(key, str):
        return key in objkey
    elif isinstance(key, tuple):
        return any(k in objkey for k in key)
    else:
        raise TypeError()


class S3Sync:
    def __init__(self, **kwargs):
        self._s3 = boto3.resource('s3', **kwargs)

    def sync_s3_to_folder(self, s3_folder, bucket_name, local_dir=None, must_keys=None,
                          ignore_keys=None, dry_run=False, overwrite=False,
                          ):
        """
        Download the contents of a folder directory
        Args:
            s3_folder: the folder path in the s3 bucket
            bucket_name: the name of the s3 bucket
            local_dir: a relative or absolute directory path in the local file system
        """
        bucket = self._s3.Bucket(bucket_name)
        for obj in bucket.objects.filter(Prefix=s3_folder):
            target = obj.key if local_dir is None \
                else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
            directory = os.path.dirname(target)
            if not os.path.exists(directory):
                os.makedirs(directory)
            if obj.key[-1] == '/':  # This is a directory, already created just above
                continue
            if must_keys and not(all([compare(obj.key, key) for key in must_keys])):
                continue
            if ignore_keys and any([compare(obj.key, key) for key in ignore_keys]):
                continue
            if os.path.exists(target) and not(overwrite):
                continue
            if dry_run:
                print("Would download {}".format(obj.key))
            else:
                print("downloading {}".format(obj.key))
                bucket.download_file(obj.key, target)

    def sync_folder_to_s3(self, s3_folder, bucket_name, local_dir, must_keys=None,
                          ignore_keys=None, dry_run=False, overwrite=False,
                          ):
        bucket = self._s3.Bucket(bucket_name)
        existing = set(
            [obj.key for obj in bucket.objects.filter(Prefix=s3_folder)]
        )
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                fname = os.path.join(root, file)
                objname = fname[:] #'/'.join(fname.split('/')[1:])
                if objname in existing and not(overwrite):
                    continue
                if must_keys and not(all([compare(fname, key) for key in must_keys])):
                    continue
                if ignore_keys and any([compare(fname, key) for key in ignore_keys]):
                    continue
                if dry_run:
                    print("Would upload {}".format(fname, objname))
                else:
                    print("Uploading {}".format(fname))
                    bucket.upload_file(fname, objname)
