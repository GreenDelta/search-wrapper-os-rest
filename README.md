# Search wrapper - Implementation for opensearch rest
This project provides an implementation for the search-wrapper API using opensearch rest as a search engine.

## Build from source

#### Dependent modules
In order to build the search-wrapper-os-rest module, you will need to install the search-wrapper API and search-wrapper-os first.
This is a plain Maven projects and can be installed via `mvn install`. See the
[search-wrapper](https://github.com/GreenDelta/search-wrapper) repository and [search-wrapper-os](https://github.com/GreenDelta/search-wrapper-os) for more
information.

#### Get the source code of the application
We recommend that to use Git to manage the source code but you can also download
the source code as a [zip file](https://github.com/GreenDelta/search-wrapper-os-rest/archive/main.zip).
Create a development directory (the path should not contain whitespaces):

```bash
mkdir dev
cd dev
```

and get the source code:

```bash
git clone https://github.com/GreenDelta/search-wrapper-os-rest.git
```

#### Build
Now you can build the module with `mvn install`, which will install the module in your local maven repository.