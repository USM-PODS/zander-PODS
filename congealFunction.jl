using CSV
using DataFrames

include("DataModulePODS.jl")

buoyID = "44007" #the ID number for the buoy
txt = ".txt" #matches the file type of the text files from NDBC
h = "h" #This is an h, its just a part of the names from the NDBC maybe it means hourly?
undscore ="_" #For new naming convention
commasep = ".csv" #used to write out files as comma seperated

ERR = ["N/A", 99.0, 99.0, 99.0, 99.0] #initializes an array to hold error values for individual columns and the date error values that can't be automated #!Structure?

startYear = 1982
endYear = 2021

function NOAAcsvStitching(bouyID, startYear, endYear, writeOut = false)
    
    ERR = ["N/A", 99.0, 99.0, 99.0, 99.0] #initializes an array to hold error values for individual columns and the date error values that can't be automated #!Structure?

    str_BouyID = string(bouyID)
    str_startYear = string(startYear)

    h = "h" #This is an h, its just a part of the names from the NDBC maybe it means hourly?
    undscore ="_" #For new naming convention

    fullDF = CSV.read(str_BouyID*h*str_startYear.txt, DataFrame, header=1, delim=" ", ignorerepeated=true) #Initialize dataframe
    
    #insertcols!(fullDF, 5, :mm_mn => 99.0) #adds an error filled minute column to match structure of later years #!Structure?
    #insertcols!(fullDF, 18, :TIDE_ft => 99.00) #adds an error filled tide column to match structure of later years #!Structure?
    
    #if startYear < 1999
    #    fullDF[!,1] .+= 1900
    #end

    #allowmissing!(fullDF)

    #CSV.write("44007_1982.csv", fullDF)

    #for col in 6:ncol(fullDF) #runs through columns of dataframe excluding the date
    #    append!(ERR, maximum(fullDF[!,col])) #adds the max value of the column to the ERR array presumably the error value
    #end

    println(ERR) #prints error to make sure nothing untoward is included

    for col in 2:ncol(fullDF)#runs through the columns of the dataframe excuding the year 
        replace!(fullDF[!,col], ERR[col] => missing) #replaces the error value for each row with missing
    end

    for i in length(defaultColumns)
    
    end

    for runningYear in startYear+1:endYear  #Loop through number of years excluding the first as that is the year used to intialize the array

        yearName = string(runningYear) #turns year into a string
        pullName = buoyID*h*yearName*txt  #Assemble file name for the year in acordance with NDBC

        if runningYear > 2006  #account for the move from one row of headers to two during the year 2006
            workingDF = CSV.read(pullName, DataFrame, header=1:2, delim=" ", ignorerepeated=true)
        else
            workingDF = CSV.read(pullName, DataFrame, header=1, delim=" ", ignorerepeated=true)
        end

        if runningYear <2005  #add a column full of error values to years before 2005 to account for lack of minutes
            insertcols!(workingDF, 5, :mm_mn => 99.0)
        end

        if runningYear <2000 #add a column full of error values to years before 2000 to account for lack of TIDE
            insertcols!(workingDF, 18, :TIDE_ft => 99.00)
        end

        if runningYear <1999
            workingDF[!,1] .+= 1900
        end

        if names(fullDF) != names(workingDF) #accounts for missmatched headers by assigning older frames the modern header
            rename!(fullDF,names(workingDF)) #from what I can tell the values are the same but the headers are different
        end                              

        allowmissing!(workingDF) #changes column type to allow interaction with missing values

        for col in 2:ncol(workingDF) #runs through columns excluding the year
            replace!(workingDF[!,col], ERR[col] => missing) #replaces each column's error values with missing
        end

        pushName = buoyID*undscore*yearName*commasep #sets a new name for the new comma separated files

        #CSV.write(pushName, workingDF)

        append!(fullDF, workingDF) #attaches the corrected year to the running dataframe including all years
    end

    return fullDF
end


CSV.write("44007_1982-2021.csv", fullDF)