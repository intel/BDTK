# How to integrate BDTK into Gluten

## Gluten code change
apply **gluten_poc.patch** to gluten code base. This patch only touches 
1. *VeloxBackend.cc*, which will translate some plan nodes to cider plan node and offload to cider . 
2. *cpp/velox/CMakeLists.txt*, which link libvelox.so with BDTK related libraries.

## BDTK code change
To be refined.

## Compile and run
compile order:  
velox -> BDTK(cider, cider-velox) -> gluten.  
To be refined.


