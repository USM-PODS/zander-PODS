using CSV
using DataFrames
using Plots
using Dates
using FFTW
using SignalAnalysis
using DSP
using CurveFit

data_44007 = CSV.read("44007_1982-2021.csv", DataFrame)

function plotColumn(data, minyear, maxyear, col, plotNum = 1)
    
    println("I'm working on " * names(data)[col] * " :)")

    section = (nrow(data)-1)/plotNum
    plotSectionLims = [1]

    for n in 1:plotNum
        append!(plotSectionLims, section*n)
    end
        temp_readings = []
        formattedDays = []

    for i in 1:(nrow(data)-1)
        currentYear = data[i,1]
        currentMonth = data[i,2]
        currentDay = data[i,3]
        currentHour= data[i,4]

        if (minyear <= currentYear <= maxyear)

            formattedDay = DateTime(currentYear, currentMonth, currentDay, currentHour)

            #if (typeof(data[i,col]) != Missing)
            #
            #    append!(temp_readings, data[i, col])
            #    push!(formattedDays, formattedDay) 
            #end  

            if (currentYear == 2013 && 
                currentMonth ==11)
            elseif (currentYear == 2013 && 
                currentMonth ==12)
            elseif (currentYear == 2010 && 
                currentMonth == 5)
            elseif (currentYear == 2010 && 
                currentMonth == 4)
            elseif (typeof(data[i,col]) != Missing)
                append!(temp_readings, data[i, col])
                push!(formattedDays, formattedDay)
            end
        end
    end

    return temp_readings, formattedDays
end;

temp, days = plotColumn(data_44007, 2017, 2017, 15)

scatter(days, temp, 
title = "Water Temperature Overtime [BuoyID: 44007]", 
xlims = (first(days), last(days)),
legend = false, size = (1000,400), msc=:transparent, markersize = 2)

##temp = convert(AbstractArray{Float64},temp)
##
##typeof(temp)
##
##ffttemp = fft(temp)
##
##psd(temp, seriestype = "scatter" , xlims = (0.0, 0.1),
##ylims = (-50,50))
##
##specgram(temp_reading)
##
#tempRFFT = rfft(temp)
#temp_filtered = filter(LowpassFilter(0.1), temp)

function approximateExtrema(buoyID, col, startYear, endYear)
    
    maxReadings = []
    minReadings = []

    for i in startYear:endYear
        yearFileName= string(buoyID)*"_"*string(i)*".csv"
        data = CSV.read(yearFileName, DataFrame)

        existing = filter(!ismissing, data[:,col])

        append!(maxReadings, maximum(existing))
        append!(minReadings, minimum(existing))
    end

    maxAvg = sum(maxReadings)/length(maxReadings)
    minAvg = sum(minReadings)/length(minReadings)

    return [maxAvg, minAvg], [maxReadings, minReadings]
end
    
avgExtrema, extrema = approximateExtrema(44007, 15, 1982, 1992)

hline!([avgExtrema], color=:darkred)

function extremeReadings(buoyID, col, avgExtrema, startYear, endYear)

    largeExtremes = []
    smallExtremes = []
    yearLarges = []
    yearSmalls = []

    for i in startYear:endYear

        yearFileName= string(buoyID)*"_"*string(i)*".csv"
        data = CSV.read(yearFileName, DataFrame)
        
        existing = filter(!ismissing, data[:,col])

        if count(existing .> avgExtrema[1]) > 0
            append!(largeExtremes, count(existing .> avgExtrema[1]))
            append!(yearLarges, i)
        elseif count(existing.< avgExtrema[2]) > 0
            append!(smallExtremes, count(existing.< avgExtrema[2]))
            append!(yearSmalls, i)
        end
    end
    return [largeExtremes, smallExtremes], [yearLarges, yearSmalls]
end

numExtremes, years = extremeReadings(44007, 15, avgExtrema, 1982,2020)
println(numExtremes)

bar(years[1], numExtremes[1])

fit = exp_fit(years[1], numExtremes[1])

expFit(fittedX) = fit[1]*exp(fit[end]*x)
range(first(years[1]), last(years[1]))

#bar(range(first(years[1]), last(years[1])), expFit.(x))
#fit = linear_fit(years[1], numExtremes[1])

#plot!([first(years[1]), last(years[1])], [(first(years[1])*fit[end]) + fit[1], (last(years[1])*fit[end]) + fit[1]])
#
#line(x) = 6x + 1
#
#x = range(1,10)
#
#linearData = line.(x)
#
#linear_fit(x,linearData)