__precompile__()
module MDP_HSM
using DataFrames
using JuMP
using Cbc
using Query
using DataValues
using CSV
if VERSION >v"0.7.0-"
    using Dates
end

export MDP_HSM_Model,RunModel

struct MDP_HSM_Model
    workFolder::String
    logFile::IOStream
    dateFrmt::DateFormat
    dfOrders::DataFrame
    dfRounds::DataFrame# for each round min max volume + min max occurence
    dfFlows::DataFrame# for each flow min max volume
    params::Dict{String,String}
end

CSV2DF(path::AbstractString)= CSV.read(path)

#would be great to precomplie this one ...
function df2ParamDict(dfParams::DataFrame)
    params=@from rds in dfParams begin
        @select rds.Key=>rds.Value
        @collect Dict
    end
    return params::Dict{String,String}
end
toList(x)=split(x,"#")

#function MDP_HSM_Model(path::AbstractString; orderFile::AbstractString="HSMOrders.csv", roundFile::AbstractString="HSMRounds.csv",flowFile::AbstractString="HSMFlows.csv",paramFile::AbstractString="HSMParams.csv",dateFrmt::DateFormat = DateFormat("mm/dd/yyyy"))
function MDP_HSM_Model(path::String; orderFile::String="HSMOrders.csv", roundFile::String="HSMRounds.csv",flowFile::String="HSMFlows.csv",paramFile::String="HSMParams.csv",dateFrmt::DateFormat = DateFormat("mm/dd/yyyy"))
    logFile=open(joinpath(path, "Logging.txt"),"w")

    write(logFile, "Reading $path $roundFile\r\n")
    dfRounds=CSV2DF(joinpath(path,roundFile))

    write(logFile,"Reading $path $flowFile\r\n")
    dfFlows=CSV2DF(joinpath(path,flowFile))

    if(false && :Active in names(dfFlows))
        dfFlows=@from fl in dfFlows begin
        @where fl.Active==1
        @select fl
        @collect DataFrame
        end
    end

    write(logFile, "Reading $path $paramFile\r\n")
    dfParams=CSV2DF(joinpath(path,paramFile))

    params=df2ParamDict(dfParams)

    write(logFile, "Reading $path $orderFile\r\n")
    dfOrders=CSV.read(joinpath(path,orderFile);delim=";",types=Dict("Works_Order_No"=>Union{String,Missing},"Expedite_Level"=>Union{String,Missing},"Furnace_Group"=>Union{String,Missing},"Galv_Options"=>Union{String,Missing},"CULPST"=>Union{Date,Missing},"HSM_LPST"=>Union{Date,Missing}),dateformat=dateFrmt)
    #showall(dfOrders)

    #remove trials , invalid ROUND_ID
    dfOrders=@from ord in dfOrders begin
        @where (ord.Customer_Name != "AM/NS Calvert Quality Internal Trials") && (!(ord.Round_ID in("NOK_Width" ,"NOK_MES Unavailable","NOK_No Location","NOK_River Terminal")))
        @select ord
        @collect DataFrame
    end

    if (!(:Flows in names(dfOrders)) && (:Flow in names(dfOrders)))
        write(logFile,"Using <Flow> for <Flows>\r\n")
        rename!(dfOrders,(:Flow=>:Flows))
    end

    dfOrders[:FlowList] = map( (x) -> toList(x),dfOrders[:Flows])
    dfOrders[:RoundList]= map( (x) -> toList(ismissing(x) ? "???" : replace(x,"IF_BH" => "IF")),dfOrders[:Round_Type])
    dfOrders[:RoundList]= map( (x) -> ("IF" in(x)) ? push!(x,"IF_Exposed") : x ,dfOrders[:RoundList])

    if (!(:Volume in names(dfOrders)) && (:Slab_Weight in names(dfOrders)))
        dfOrders[:Volume] = map( (x) -> x/1000,dfOrders[:Slab_Weight])
        dfOrders[:WidthGroup] =map( (x) -> floor(Int,x/100),dfOrders[:Aim_Width])
    end

    #println(eltypes(dfOrders))
    #showall(dfOrders)

    res= MDP_HSM_Model(path,logFile,dateFrmt,dfOrders,dfRounds,dfFlows,params)
    write(logFile,"Model created\r\n")
    return res
end

#precomp hint

CSV2DF(joinpath(@__DIR__,"../data/HSMParams.csv"))
#no gain
#toList("GI")
# not working... [1] query(::DataFrames.DataFrame) at C:\Users\10500508\.julia\v0.6\QueryOperators\src\source_iterable.jl:
#df2ParamDict(CSV2DF(joinpath(@__DIR__,"../data/HSMParams.csv")))

function orderInFlow(orderFlow::Array, flow::AbstractString)
#println(typeof(orderFlow))
if in(flow, orderFlow)
    return 1
end
flowSum=split(flow,"+")# we have a case like GI_3+GI_34
if length(flowSum)==1
    return 0
end
for f in flowSum
    if in(f, orderFlow)
        #println(orderFlow, " in ", f)
        return 1
    end
end
return 0
end
#orderInFlow("GI3","GI3+GI34")#precomp hint

function RunModel(aMDPModel::MDP_HSM_Model;RoundLimitsAsConstraint::Bool=true)
#write(aMDPModel.logFile,"Building Model\r\n")
m=Model(with_optimizer(Cbc.Optimizer ,logLevel=1))
nOrders=size(aMDPModel.dfOrders,1)

@variable(m,VolInRd[i=1:nOrders,r in aMDPModel.dfOrders[i,:RoundList]]>=0)
@variable(m,VolOuterBayInRd[i=1:nOrders,r in aMDPModel.dfOrders[i,:RoundList]]>=0)

@variable(m,RdPerWidthGroupLength[r in aMDPModel.dfRounds[:RoundName],w=1:20]>=0)

@variable(m,Flow[f in aMDPModel.dfFlows[:FlowName]]>=0)

@variable(m,FlowShortage[f in aMDPModel.dfFlows[:FlowName]]>=0)
@variable(m,FlowExcess[f in aMDPModel.dfFlows[:FlowName]]>=0)


minOcc=@from rds in aMDPModel.dfRounds begin
    @select rds.RoundName=>  rds.MinOccurence
    @collect Dict
end

maxOcc=@from rds in aMDPModel.dfRounds begin
    @select rds.RoundName=> rds.MaxOccurence
    @collect Dict
end

MinVolumePerRound=@from rds in aMDPModel.dfRounds begin
    @select rds.RoundName=>rds.MinVolPerRound
    @collect Dict
end

MaxVolumePerRound=@from rds in aMDPModel.dfRounds begin
    @select rds.RoundName=>rds.MaxVolPerRound
    @collect Dict
end

OuterBayVolumePerRound=@from rds in aMDPModel.dfRounds begin
    @select rds.RoundName=>rds.OuterbayVol
    @collect Dict
end

#show(aMDPModel.dfFlows, allrows=true,allcols=true)
minFlow=@from fl in aMDPModel.dfFlows begin
    @select fl.FlowName=>get(fl.FlowMin, 0)
    @collect Dict
end
#show(minFlow)

maxFlow=@from fl in aMDPModel.dfFlows begin
    @select fl.FlowName=>get(fl.FlowMax,0)
    @collect Dict
end

@variable(m, Rd[r in aMDPModel.dfRounds[:RoundName]] <=maxOcc[r], lower_bound=minOcc[r], Int)

@variable(m,RdShortage[r in aMDPModel.dfRounds[:RoundName]]>=0)
@variable(m,RdExcess[r in aMDPModel.dfRounds[:RoundName]]>=0)

for i=1:nOrders
    @constraint(m,sum(VolInRd[i,r] for r in aMDPModel.dfOrders[i,:RoundList]) <= aMDPModel.dfOrders[i,:Volume])
#unknown flow/round
    for r in aMDPModel.dfOrders[i,:RoundList]
        if !haskey( minOcc,r)
            println("Order $i has invalid round $r")
            @constraint(m,VolInRd[i,r] ==0 )
        end
        @constraint(m,VolOuterBayInRd[i,r]==(aMDPModel.dfOrders[i,:Yard_Location]  =="OuterBay" ? VolInRd[i,r] : 0))
    end
    #for f in aMDPModel.dfOrders[i,:FlowList]
    #    if !haskey(minFlow,f)
    #        @constraint(m,sum(VolInRd[i,r] for r in aMDPModel.dfOrders[i,:RoundList]) ==0 )
    #    end
    #end
end

if haskey(aMDPModel.params,"OuterbayVol")
    @constraint(m,sum(VolOuterBayInRd[i,r] for i=1:nOrders, r in (aMDPModel.dfOrders[i,:RoundList]) ) <= parse(Float64, aMDPModel.params["OuterbayVol"]))
end

@constraint(m, sum(Rd[r] for r in aMDPModel.dfRounds[:RoundName]) >= parse(Int32, aMDPModel.params["Min_Rounds"]))
@constraint(m, sum(Rd[r] for r in aMDPModel.dfRounds[:RoundName]) <= parse(Int32, aMDPModel.params["Max_Rounds"]))


@constraint(m, conRdPerWidthGroupLength[r in (aMDPModel.dfRounds[:RoundName]) ,w=1:20] ,RdPerWidthGroupLength[r,w] == sum(VolInRd[i,r]/aMDPModel.dfOrders[i,:Volume]*aMDPModel.dfOrders[i,:Coil_Length] for i=1:nOrders if dfOrders[i,:WidthGroup]==w && r in aMDPModel.dfOrders[i,:RoundList]))

if haskey(aMDPModel.params,"WidthGroupLimit") && aMDPModel.params["WidthGroupLimit"]!=0
    @constraint(m, conRdPerWidthGroupLengthLimit[r in (aMDPModel.dfRounds[:RoundName]) ,w=1:20] ,RdPerWidthGroupLength[r,w] <=55*Rd[r])
end

@variable(m,RdVol[r in aMDPModel.dfRounds[:RoundName]])
@variable(m,totalVol)

for r in aMDPModel.dfRounds[:RoundName]
    @constraint(m,RdVol[r]==sum(VolInRd[i,r] for i=1:nOrders if r in aMDPModel.dfOrders[i,:RoundList]))
    @constraint(m,RdShortage[r] >= MinVolumePerRound[r]*Rd[r] - RdVol[r])
    @constraint(m,RdExcess[r]>=RdVol[r] - Rd[r]*MaxVolumePerRound[r] )
    if RoundLimitsAsConstraint
        @constraint(m,RdShortage[r]==0)
        @constraint(m,RdExcess[r]==0)
    end
    @constraint(m,sum(VolOuterBayInRd[i,r] for i=1:nOrders if r in aMDPModel.dfOrders[i,:RoundList])<=OuterBayVolumePerRound[r])
end

@constraint(m,totalVol==sum(RdVol[r] for r in (aMDPModel.dfRounds[:RoundName]) ))
if haskey(aMDPModel.params,"MinVolume")
    @constraint(m,totalVol >= parse(Float64, aMDPModel.params["MinVolume"]))
end
if haskey(aMDPModel.params,"MaxVolume")
    @constraint(m,totalVol <= parse(Float64, aMDPModel.params["MaxVolume"]))
end


for f in aMDPModel.dfFlows[:FlowName]
    #@constraint(m,Flow[f]==sum(VolInRd[i,r]*in(f,aMDPModel.dfOrders[i,:FlowList]) for i=1:nOrders, r in aMDPModel.dfOrders[i,:RoundList]))
    @constraint(m,Flow[f]==sum(VolInRd[i,r]* orderInFlow(aMDPModel.dfOrders[i,:FlowList],f) for i=1:nOrders, r in aMDPModel.dfOrders[i,:RoundList]))
    @constraint(m,FlowShortage[f]>=minFlow[f]-Flow[f])
    @constraint(m,FlowExcess[f]>=Flow[f]-maxFlow[f])
end

@variable(m,flowShortagecost[f in aMDPModel.dfFlows[:FlowName]])
@variable(m,flowExcesscost[f in aMDPModel.dfFlows[:FlowName]])
@variable(m,totalflowShortagecost)
@variable(m,totalflowExcesscost)

FlowShortageCostFactor=@from fl in aMDPModel.dfFlows begin
    @select fl.FlowName=>fl.FlowShortageCostFactor*fl.Active
    @collect Dict
end

FlowExcessCostFactor=@from fl in aMDPModel.dfFlows begin
    @select fl.FlowName=>fl.FlowExcessCostFactor*fl.Active
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

@objective(m,Min,totalflowExcesscost + totalflowShortagecost + totalSelectionCost + totalOuterbayCost)
write(aMDPModel.logFile,"Solving Model\r\n")

modelPrint=open(joinpath(aMDPModel.workFolder,"model.txt"),"w")

println(modelPrint,m)
optimize!(m)
stat = termination_status(m)
write(aMDPModel.logFile,"Solved Model: $stat\r\n")
result=open(joinpath(aMDPModel.workFolder,"Result.csv"),"w")
    println(result,"Status,$stat\r")

if stat==MOI.OPTIMAL
    println(result,"Cost,",objective_value(m),"\r")
    println(result,"FlowExcessCost,",value(totalflowExcesscost),"\r")
    println(result,"FlowShortageCost,",value(totalflowShortagecost),"\r")
    println(result,"SelectionCost,",value(totalSelectionCost),"\r")

    close(result)

    flowResult=open(joinpath(aMDPModel.workFolder,"HSMFlows_Result.csv"),"w")
    #write Flows
    for f in aMDPModel.dfFlows[:FlowName]
        val =value(Flow[f])
        shortage=value(FlowShortage[f])
        excess=value(FlowExcess[f])
        write(flowResult,"$f,$val,$shortage,$excess\r\n")
    end
    close(flowResult)
    #write rounds
    roundResult=open(joinpath(aMDPModel.workFolder,"HSMRounds_Result.csv"),"w")
    for r in aMDPModel.dfRounds[:RoundName]
        val=value(Rd[r])
        vol=value(RdVol[r])
        write(roundResult,"$r,$val,$vol\r\n")
        #println(r,",",value(Rd[r]))
    end
    close(roundResult)
    #write Orders
    orderResult=open(joinpath(aMDPModel.workFolder,"HSMOrders_Result.csv"),"w")
    for i=1:nOrders
        vol=0
        for r in aMDPModel.dfOrders[i,:RoundList]
            if (value(VolInRd[i,r])>0)
               #println("\t$i,$r," , aMDPModel.dfOrders[i,:Works_Order_No] , ",$vol")
                vol += value(VolInRd[i,r])
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
close(aMDPModel.logFile)
return stat
end
end
