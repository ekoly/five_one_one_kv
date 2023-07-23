"""
C implemented KV store.
"""
import setuptools

setuptools.setup(
    packages=[
        "five_one_one_kv",
    ],
    package_dir={
        "five_one_one_kv": "python",
    },
    ext_modules=[
        setuptools.Extension(
            "five_one_one_kv.c",
            [
                "server/util.c",
                "server/server.c",
                "server/dispatch.c",
                "server/module.c",
            ],
        ),
    ],
)
