""" Invoke Tasks to help build the kafka to hbase utility """
import os as _os

from invoke import Collection

from . import dev, test, hbase

# Set working director to the root of the repo
_os.chdir(
    _os.path.join(
        _os.path.dirname(__file__),
        ".."
    )
)

namespace = Collection()
namespace.add_collection(Collection.from_module(dev))
namespace.add_collection(Collection.from_module(test))
namespace.add_collection(Collection.from_module(hbase))
