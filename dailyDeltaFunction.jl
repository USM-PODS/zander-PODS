using CSV
using DataFrames
using Plots
using Dates
using SignalAnalysis
using FFTW
using NFFT

data_44007 = CSV.read("44007_1982-2021.csv", DataFrame)
#data_IOSN3 = CSV.read("iosn3_1982-2021.csv", DataFrame)

function diurnalAmp(data, startYear, endYear, col)
    Δ = [] 
    todayData = []
    formattedDays = []
    errΔ = []
    errDates = []

    indexDays = []

    for i in 1:(nrow(data)-1)

        currentYear = data[i,1]
        currentMonth = data[i,2]
        currentDay = data[i,3]

        formattedDay = Date(currentYear, currentMonth, currentDay)
    
        if startYear <= currentYear <= endYear

            if isempty(formattedDays) == true #initializes formattedDays

                push!(formattedDays, formattedDay)

            end

            if formattedDay != last(formattedDays) && typeof(data[i,col]) != Missing

                δ = maximum(todayData) - minimum(todayData)
                
                append!(Δ, δ)
                append!(indexDays, i)
                
                if δ >= 10
                    push!(errΔ, δ)
                    push!(errDates, last(formattedDays))
                end
                todayData = []
                push!(formattedDays, formattedDay)
            end

            if typeof(data[i,15]) != Missing
                append!(todayData, data[i,col])
            end
        end
    end


    δ = maximum(todayData) - minimum(todayData)
    append!(Δ, δ)
    return Δ, formattedDays, errΔ, errDates, indexDays
end;

Δ, formattedDays, errΔ, errDates, indexDays = diurnalAmp(data_44007, 1882, 2020, 15)

length(Δ)
length(formattedDays)
scatter(formattedDays, Δ)

length(errΔ)
length(errDates)
println(errΔ)
println(errDates)

Δ_float = convert(Vector{Float64}, Δ)

nfft2(Δ)

