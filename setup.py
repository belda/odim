import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="odim",
    version="0.1.34",
    author=[
        "Belda", 
        "JH"
        ],
    author_email=[
        "jakub.belescak@centrum.cz",
        "john@huseniovic.net"
        ],
    description="Simple Python ORM/ODM specifically designed to be used with Pydantic and FastAPI ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/belda/odim",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires = [
        "pydantic",
        "nest_asyncio"
    ]
)