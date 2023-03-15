using CSV
using DataFrames

include("DataModulePODS.jl")

#! buoyID = "44007" #the ID number for the buoy
#! txt = ".txt" #matches the file type of the text files from NDBC
#! h = "h" #This is an h, its just a part of the names from the NDBC maybe it means hourly?
#! undscore ="_" #For new naming convention
#! commasep = ".csv" #used to write out files as comma seperated

#! ERR = ["N/A", 99.0, 99.0, 99.0, 99.0] #initializes an array to hold error values for individual columns and the date error values that can't be automated #!Structure?

startYear = 1982
endYear = 2021

function NOAAcsvStitching(buoyID, startYear, endYear, writeOut = false)
    
    ERR = [] #initializes an array to hold error values for individual columns

    str_buoyID = string(buoyID)
    
    #! str_startYear = string(startYear)

    h = "h" #This is an h, its just a part of the names from the NDBC maybe it means hourly?
    undscore ="_" #For new naming convention
    txt = ".txt" #matches the file type of the text files from NDBC
    commasep = ".csv" #used to write out files as comma seperated

    fullDF = data.frame(matrix(ncol = 18, nrow = 0))
    
    colnames(fullDF) = DefaultColumns

    #! fullDF = CSV.read(str_BouyID*h*str_startYear.txt, DataFrame, header=1, delim=" ", ignorerepeated=true) #Initialize dataframe

    #! println(ERR) #prints error to make sure nothing untoward is included

    for i in 2:length(defaultColumns)
       append!(ERR, defaultColumns[i].errorValue)
    end

    for runningYear in startYear:endYear  #Loop through number of years excluding the first as that is the year used to intialize the array

        yearName = string(runningYear) #turns year into a string
        pullName = str_buoyID*h*yearName*txt  #Assemble file name for the year in acordance with NDBC

        if runningYear > 2006  #account for the move from one row of headers to two during the year 2006
            workingDF = CSV.read(pullName, DataFrame, header=1:2, delim=" ", ignorerepeated=true)
        elseif runningYear > startYear
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
            pushName = str_buoyID*undscore*yearName*commasep #sets a new name for the new comma separated files

            CSV.write(pushName, workingDF)
        end

        append!(fullDF, workingDF) #attaches the corrected year to the running dataframe including all years
    end
    
        if writeOut == true
            CSV.write("44007_1982-2021.csv", fullDF)
        end
    return fullDF
end