__precompile__()
module MDP_HSM
using DataFrames
using JuMP
using Cbc
using Query
using CSV

export MDP_HSM_Model,RunModel

struct MDP_HSM_Model
    workFolder::AbstractString
    logFile::IOStream
    dateFrmt::DateFormat
    dfOrders::DataFrame
    dfRounds::DataFrame# for each round min max volume + min max occurence
    dfFlows::DataFrame# for each flow min max volume
    params::Dict
end

function MDP_HSM_Model(path::AbstractString; orderFile::AbstractString="HSMOrders.csv", roundFile::AbstractString="HSMRounds.csv",flowFile::AbstractString="HSMFlows.csv",paramFile::AbstractString="HSMParams.csv",dateFrmt::DateFormat = DateFormat("mm/dd/yyyy"))
    logFile=open(joinpath(path, "Logging.txt"),"w")

    write(logFile, "Reading $path $roundFile\r\n")
    dfRounds=CSV.read(joinpath(path,roundFile))

    write(logFile,"Reading $path $flowFile\r\n")
    dfFlows=CSV.read(joinpath(path,flowFile))
    write(logFile, "Reading $path $paramFile\r\n")
    dfParams=CSV.read(joinpath(path,paramFile))
    params=@from rds in dfParams begin
        @select get(rds.Key)=>get(rds.Value)
        @collect Dict
    end
    write(logFile, "Reading $path $orderFile\r\n")


    dfOrders=CSV.read(joinpath(path,orderFile);delim=";",types=Dict("Works_Order_No"=>Union{String,Missing},"Expedite_Level"=>Union{String,Missing},"Furnace_Group"=>Union{String,Missing},"Galv_Options"=>Union{String,Missing},"CULPST"=>Union{Date,Missing},"HSM_LPST"=>Union{Date,Missing}),dateformat=dateFrmt)
    #showall(dfOrders)

    if (!(:Flows in names(dfOrders)) && (:Flow in names(dfOrders)))
        write(logFile,"Using <Flow> for <Flows>\r\n")
        rename!(dfOrders,(:Flow=>:Flows))
    end

    dfOrders[:FlowList] = map( (x) -> split(x,"#"),dfOrders[:Flows])
    dfOrders[:RoundList]= map( (x) -> ismissing(x)?"": split(replace(x,"IF_BH","IF"),"#"),dfOrders[:Round_Type])

    if (!(:Volume in names(dfOrders)) && ( :Slab_Weight in names(dfOrders)))
        dfOrders[:Volume] = map( (x) -> x/1000,dfOrders[:Slab_Weight])
    end

    #remove trials
    dfOrders=@from ord in dfOrders begin
        @where ord.Customer_Name != "AM/NS Calvert Quality Internal Trials"
        @select ord
        @collect DataFrame
    end
    #remove ROUND_ID
    dfOrders=@from ord in dfOrders begin
        @where !(ord.Round_ID in( "NOK_Width" ,"NOK_MES Unavailable","NOK_No Location","NOK_River Terminal"))
        @select ord
        @collect DataFrame
    end

    #println(eltypes(dfOrders))
    #showall(dfOrders)

    res= MDP_HSM_Model(path,logFile,dateFrmt,dfOrders,dfRounds,dfFlows,params)
    write(logFile,"Model created","\r\n")
    return res
end

function RunModel(aMDPModel::MDP_HSM_Model;RoundLimitsAsConstraint::Bool=true)
#write(aMDPModel.logFile,"Building Model\r\n")
m=Model(solver=CbcSolver(logLevel=1))
nOrders=size(aMDPModel.dfOrders,1)

@variable(m,VolInRd[i=1:nOrders,r in aMDPModel.dfOrders[i,:RoundList]]>=0)
@variable(m,VolOuterBayInRd[i=1:nOrders,r in aMDPModel.dfOrders[i,:RoundList]]>=0)

@variable(m,Flow[f in aMDPModel.dfFlows[:FlowName]]>=0)

@variable(m,FlowShortage[f in aMDPModel.dfFlows[:FlowName]]>=0)
@variable(m,FlowExcess[f in aMDPModel.dfFlows[:FlowName]]>=0)

minOcc=@from rds in aMDPModel.dfRounds begin
    @select get(rds.RoundName)=>get(rds.MinOccurence,0)
    @collect Dict
end

maxOcc=@from rds in aMDPModel.dfRounds begin
    @select get(rds.RoundName)=>get(rds.MaxOccurence)
    @collect Dict
end

MinVolumePerRound=@from rds in aMDPModel.dfRounds begin
    @select get(rds.RoundName)=>get(rds.MinVolumePerRound)
    @collect Dict
end

MaxVolumePerRound=@from rds in aMDPModel.dfRounds begin
    @select get(rds.RoundName)=>get(rds.MaxVolumePerRound)
    @collect Dict
end

OuterBayVolumePerRound=@from rds in aMDPModel.dfRounds begin
    @select get(rds.RoundName)=>get(rds.OuterbayVolume)
    @collect Dict
end

minFlow=@from fl in aMDPModel.dfFlows begin
    @select get(fl.FlowName)=>get(fl.FlowMin,0)
    @collect Dict
end

maxFlow=@from fl in aMDPModel.dfFlows begin
    @select get(fl.FlowName)=>get(fl.FlowMax)
    @collect Dict
end

@variable(m, Rd[r in aMDPModel.dfRounds[:RoundName]] <=maxOcc[r], lowerbound=minOcc[r], Int)

@variable(m,RdShortage[r in aMDPModel.dfRounds[:RoundName]]>=0)
@variable(m,RdExcess[r in aMDPModel.dfRounds[:RoundName]]>=0)

for i=1:nOrders
    @constraint(m,sum(VolInRd[i,r] for r in aMDPModel.dfOrders[i,:RoundList]) <= aMDPModel.dfOrders[i,:Volume])
#unknown flow/round
    for r in aMDPModel.dfOrders[i,:RoundList]
        if !haskey( minOcc,r)
            @constraint(m,VolInRd[i,r] ==0 )
        end
        @constraint(m,VolOuterBayInRd[i,r]==(aMDPModel.dfOrders[i,:Yard_Location]  =="OuterBay"?VolInRd[i,r]:0))
    end
    for f in aMDPModel.dfOrders[i,:FlowList]
        if !haskey(minFlow,f)
            @constraint(m,sum(VolInRd[i,r] for r in aMDPModel.dfOrders[i,:RoundList]) ==0 )
        end
    end
end

if haskey(aMDPModel.params,"OuterbayVolume")
    @constraint(m,sum(VolOuterBayInRd[i,r] for i=1:nOrders, r in (aMDPModel.dfOrders[i,:RoundList]) ) <= parse(Float64, aMDPModel.params["OuterbayVolume"]))
end

@constraint(m, sum(Rd[r] for r in aMDPModel.dfRounds[:RoundName]) >= parse(Int32, aMDPModel.params["Min_Rounds"]))
@constraint(m, sum(Rd[r] for r in aMDPModel.dfRounds[:RoundName]) <= parse(Int32, aMDPModel.params["Max_Rounds"]))

for r in aMDPModel.dfRounds[:RoundName]
    @constraint(m,RdShortage[r] >= MinVolumePerRound[r]*Rd[r] - sum(VolInRd[i,r] for i=1:nOrders if r in aMDPModel.dfOrders[i,:RoundList]))
    @constraint(m,RdExcess[r]>=sum(VolInRd[i,r] for i=1:nOrders if r in aMDPModel.dfOrders[i,:RoundList])- Rd[r]*MaxVolumePerRound[r] )
    if RoundLimitsAsConstraint
        @constraint(m,RdShortage[r]==0)
        @constraint(m,RdExcess[r]==0)
    end
    @constraint(m,sum(VolOuterBayInRd[i,r] for i=1:nOrders if r in aMDPModel.dfOrders[i,:RoundList])<=OuterBayVolumePerRound[r])
end

for f in aMDPModel.dfFlows[:FlowName]
    @constraint(m,Flow[f]==sum(VolInRd[i,r]*in(f,aMDPModel.dfOrders[i,:FlowList]) for i=1:nOrders, r in aMDPModel.dfOrders[i,:RoundList]))
    @constraint(m,FlowShortage[f]>=minFlow[f]-Flow[f])
    @constraint(m,FlowExcess[f]>=Flow[f]-maxFlow[f])
end

@variable(m,flowShortagecost[f in aMDPModel.dfFlows[:FlowName]])
@variable(m,flowExcesscost[f in aMDPModel.dfFlows[:FlowName]])
@variable(m,totalflowShortagecost)
@variable(m,totalflowExcesscost)

FlowShortageCostFactor=@from fl in aMDPModel.dfFlows begin
    @select get(fl.FlowName)=>get(fl.FlowShortageCostFactor)
    @collect Dict
end

FlowExcessCostFactor=@from fl in aMDPModel.dfFlows begin
    @select get(fl.FlowName)=>get(fl.FlowExcessCostFactor)
    @collect Dict
end

for f in aMDPModel.dfFlows[:FlowName]
    @constraint(m,flowShortagecost[f]==sum(FlowShortage[f]*FlowShortageCostFactor[f] ))
    @constraint(m,flowExcesscost[f]==sum(FlowExcess[f]*FlowExcessCostFactor[f] ))
end

@constraint(m,totalflowShortagecost==sum(flowShortagecost[f] for f in aMDPModel.dfFlows[:FlowName]))
@constraint(m,totalflowExcesscost==sum(flowExcesscost[f] for f in aMDPModel.dfFlows[:FlowName]))

@variable(m,totalSelectionCost)
@constraint(m,totalSelectionCost==sum(VolInRd[i,r]*aMDPModel.dfOrders[i,:SelectionCost] for i=1:nOrders,r in aMDPModel.dfOrders[i,:RoundList]))
@variable(m,totalOuterbayCost)
if ! haskey(aMDPModel.params,"OuterbayCost")
    @constraint(m,totalOuterbayCost==0)
else
    @constraint(m,totalOuterbayCost==sum(VolOuterBayInRd[i,r])*aMDPModel.params["OuterbayCost"])
end


@objective(m,:Min,totalflowExcesscost+totalflowShortagecost+totalSelectionCost+totalOuterbayCost)
write(aMDPModel.logFile,"Solving Model\r\n")

#println(m)
ModelStatus=solve(m)
write(aMDPModel.logFile,"Solved Model: $ModelStatus\r\n")
result=open(joinpath(aMDPModel.workFolder,"Result.csv"),"w")
    println(result,"Status,$ModelStatus\r")

if ModelStatus==:Optimal
    println(result,"Cost,",getobjectivevalue(m),"\r")
    close(result)

    flowResult=open(joinpath(aMDPModel.workFolder,"HSMFlows_Result.csv"),"w")
    #write Flows
    for f in aMDPModel.dfFlows[:FlowName]
        val =getvalue(Flow[f])
        shortage=getvalue(FlowShortage[f])
        excess=getvalue(FlowExcess[f])
        write(flowResult,"$f,$val,$shortage,$excess\r\n")
    end
    close(flowResult)
    #write rounds
    roundResult=open(joinpath(aMDPModel.workFolder,"HSMRounds_Result.csv"),"w")
    for r in aMDPModel.dfRounds[:RoundName]
        val=getvalue(Rd[r])
        write(roundResult,"$r,$val\r\n")
        #println(r,",",getvalue(Rd[r]))
    end
    close(roundResult)
    #write Orders
    orderResult=open(joinpath(aMDPModel.workFolder,"HSMOrders_Result.csv"),"w")
    vol=0
    for i=1:nOrders
        vol=0
        for r in aMDPModel.dfOrders[i,:RoundList]
            if (getvalue(VolInRd[i,r])>0)
               #println("\t$i,$r," , aMDPModel.dfOrders[i,:Works_Order_No] , ",$vol")
                vol += getvalue(VolInRd[i,r])
            end
        end
        if vol>0
            println(orderResult,aMDPModel.dfOrders[i,:Works_Order_No],",$vol\r")
        end
    end
    close(orderResult)
end
println(result,"Cost,",999999999999999999999,"\r")
close(result)

return ModelStatus
end

end
