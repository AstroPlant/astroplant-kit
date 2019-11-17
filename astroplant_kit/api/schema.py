import capnp

capnp.remove_import_hook()
astroplant_capnp = capnp.load("./proto/astroplant.capnp")
