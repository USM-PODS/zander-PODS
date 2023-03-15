using CSV
using DataFrames
using Plots

data_44007 = CSV.read("44007_1982-2021.csv", DataFrame)

WTMP = []

for i in 1:(nrow(data_44007)-1)
    if typeof(data_44007[i, 15]) != Missing
        append!(WTMP, data_44007[i, 15])
    end
end

function PDF(array, numBins)

    Δ = maximum(array) - minimum(array)
    binSize = Δ/numBins
    mean = sum(array)/length(array)
    N = []
    bins = []
    ΔMean = []

    for i in 1:numBins
        n = 0

        max  = i*binSize
        min = (i-1)*binSize
        append!(bins, max)

        for i in 1:length(array)
            if min <= array[i] <= max
                n = n + 1
            end
            δMean = array[i] - mean 
            append!(ΔMean, δMean)
        end

        append!(N, n)
    end
    return bins, N, mean, sum(ΔMean.^2)/length(array)^(0.5)
end

tempBins, tempN, tempAvg, tempSigma = PDF(WTMP, 200)

scatter(tempBins, tempN)
println(tempAvg)