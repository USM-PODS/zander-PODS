using CSV
using DataFrames

include("DataModulePODS.jl")

function NOAAcsvStitching(buoyID, startYear, endYear, writeOut = false)
    
    ERR = [] #initializes an array to hold error values for individual columns

    fullDF = DataFrame(
        I      = [],
        II     = [],
        III    = [],
        IV     = [],
        V      = [],
        VI     = [],
        VII    = [],
        VIII   = [],
        IX     = [],
        X      = [],
        XI     = [],
        XII    = [],
        XIII   = [],
        XIV    = [],
        XV     = [],
        XVI    = [],
        XVII   = [],
        XVIII  = [],
    )

    for i in 1:length(DefaultColumns)
       append!(ERR, DefaultColumns[i].errorValue)
    end

    for runningYear in startYear:endYear  #Loop through number of years excluding the first as that is the year used to intialize the array

        yearName = string(runningYear) #turns year into a string
        pullName = string(buoyID)*"h"*yearName*".txt"  #Assemble file name for the year in acordance with NDBC

        if runningYear > 2006  #account for the move from one row of headers to two during the year 2006
            workingDF = CSV.read(pullName, DataFrame, header=1:2, delim=" ", ignorerepeated=true)
        else
            workingDF = CSV.read(pullName, DataFrame, header=1, delim=" ", ignorerepeated=true)
        end

        if runningYear < 2005  #add a column full of error values to years before 2005 to account for lack of minutes
            insertcols!(workingDF, 5, :mm_mn => 99.0)
        end

        if runningYear < 2000 #add a column full of error values to years before 2000 to account for lack of TIDE
            insertcols!(workingDF, 18, :TIDE_ft => 99.00)
        end

        if runningYear < 1999
            workingDF[!,1] .+= 1900
        end

        if names(fullDF) != names(workingDF) #accounts for missmatched headers by assigning older frames the modern header
            rename!(fullDF,names(workingDF)) #from what I can tell the values are the same but the headers are different
        end                              

        allowmissing!(workingDF) #changes column type to allow interaction with missing values

        for col in 2:ncol(workingDF) #runs through columns excluding the year
            replace!(workingDF[!,col], ERR[col] => missing) #replaces each column's error values with missing
        end

        if writeOut == true
            pushName = string(buoyID)*"_"*yearName*".csv" #sets a new name for the new comma separated files

            CSV.write(pushName, workingDF)
        end

        append!(fullDF, workingDF) #attaches the corrected year to the running dataframe including all years
    end
    
    if writeOut == true
        CSV.write(string(buoyID) * "_" * string(startYear) * "-"* string(endYear)* ".csv", fullDF)
    end
end

NOAAcsvStitching(44027, 2003, 2022, true)

