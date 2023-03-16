using DataFrames
using CSV

df1 = DataFrame(a = [])
df2 = DataFrame(b= [])

DF = hcat(df1, df2)

println(DF)