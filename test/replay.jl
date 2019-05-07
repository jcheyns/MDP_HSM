#include("C:/Users/10500508/Documents/julia/MDP_HSM/MDP_HSM_Model.jl")
using MDP_HSM
#global_logger(SimpleLogger(open(joinpath(path, "Logging.txt"),"w")))

if size(ARGS,1)==0
    myModel=MDP_HSM.MDP_HSM_Model("C:/tmp/20180801085842")
elseif size(ARGS,1)==1
    arg=ARGS[1]
    #error(arg)
    myModel=MDP_HSM.MDP_HSM_Model(arg)
else
    nArgs=size(ARGS,1)
    for i=1:nArgs
        println(ARGS[i])
    end
    error("0 or 1 arguments expected, received $nArgs")
end

MDP_HSM.RunModel(myModel) ==:Optimal #|| MDP_HSM.RunModel(myModel,false)
close(myModel.logFile)
