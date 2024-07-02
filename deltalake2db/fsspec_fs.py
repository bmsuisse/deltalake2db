def get_az_blob_fs(fake_proto: str, **kwargs):
    import adlfs

    class AzureBlobFS(adlfs.AzureBlobFileSystem):
        protocol = fake_proto

        def __init__(self, real_proto, *args, **kwargs):
            super().__init__(*args, **kwargs)

        @classmethod
        def _strip_protocol(cls, path: str):
            path = path.replace(fake_proto, "abfs")
            return super()._strip_protocol(path)

    return AzureBlobFS("abfs", **kwargs)
