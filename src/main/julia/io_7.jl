# another failed bid to improve writing to dict

using CSV, DataFrames

const NEWLINE =  "\n"[1] |> UInt8
const SEMICOL =  ";"[1] |> UInt8
const PERIOD = "."[1] |> UInt8
const DASH = "-"[1] |> UInt8
const CARRIAGE = "\r"[1] |> UInt8
const SPACE = " "[1] |> UInt8
const TAB = "\t"[1] |> UInt8

names = CSV.File("data/weather_stations_clean.csv", header=["name", "x"]).name

# for testing
function hash(str, p1=11, p2=53)
    munch_name(Vector{UInt8}("$(str);"), 1, p1, p2)[1]
end


@inline function munch_name(buffy, i, p1=11, p2=53)
    # p1 and p2 are params that adjust the hashing
    @inbounds begin
        # creates a shitty hash of the name encoded starting at buffy[i] until the next semi colon from position i 
        # returns hash and index to start parsing number at
        
        if buffy[i] == NEWLINE
            i += 1
        end

        i0 = i
        #@info "buffer here to end: $(String(buffy[i:end]))"
        out = zero(UInt64)
        while buffy[i] != SEMICOL
            if i - i0 < 8 
                shift_bits = 8 * (i - i0)
            else
                shift_bits = ((i - i0) * p1) % p2
            end
            out = out ⊻ (UInt64(buffy[i]) << shift_bits)
            i += 1
        end
        
        return out, i + 1
    end
end

@inline function munch_decimal(buffy, i)
    @inbounds begin
    # non-zero padded decimal from -99.9 to 99.9 always with one decimal place. Treat as int from -999 to 999
  #  @info "decimal start, rest of buffer: $(String(buffy[i:end]))"
    
  
    
     # get +/-
    if buffy[i] == DASH
        sign = -one(Int32)
        i += 1
    else
        sign = one(Int32)
    end

    # leading digit
    value = Int32(buffy[i] - 0x30)
    i+=1
    
    if buffy[i] == PERIOD
        # if next char is '.' then abs this number < 10
        i += 1
        value *= 10
        value += Int32(buffy[i] - 0x30)
        i += 1
    else
        # abs this number >= 10
        value *= 10
        # add next leading decimal
        value += Int32(buffy[i] - 0x30)
        
        # add two because next char guaranteed to be '.'
        i+=2

        # add final decimal place
        value *= 10
        value += Int32(buffy[i] - 0x30)
        i += 1
    end

    
    while (buffy[i] == SPACE || buffy[i] == TAB || buffy[i] == NEWLINE || buffy[i] == CARRIAGE)
        i += 1
    end
    # lines appear to end in \r then \n, so if [i+3] is the tenths place of measurement, +6 is the first char of next line
    return value * sign, i
    end
end

function process_chunk!(result_dict, buffy)
    rows = 0
    bytes_read = 1

    digests = Vector{UInt64}(undef, 8)
    values = Vector{NTuple{4, Int32}}(undef, 8)
    cache_index = 1

    function cache_or_upload(digest, value)
        digests[cache_index] = digest
        values[cache_index] = value
        cache_index += 1
        if cache_index > length(digests)
            for (digest, value) in zip(digests, values)
                if digest in keys(result_dict)
                    V = result_dict[digest]
                else
                    V = typemax(Int32), typemin(Int32), zero(Int32), zero(Int32)
                end
                V = merge_values(V, value)
                result_dict[digest] = V
            end
            cache_index = 1
        end
    end

    while bytes_read + 1 < length(buffy)
        
        digest, bytes_read = munch_name(buffy, bytes_read)
        reading, bytes_read = munch_decimal(buffy, bytes_read)
        
        reading = Int32(reading)
        
        #V = merge_values(V, (reading, reading, reading, Int32(1)))
        cache_or_upload(digest, (reading, reading, reading, Int32(1)))
       # result_dict[digest] = V
    end
end

function merge_values(v1 :: NTuple{4, T}, v2 :: NTuple{4, T}) :: NTuple{4, T} where T
    min(v1[1], v2[1]), max(v1[2], v2[2]), v1[3] + v2[3], v1[4] + v2[4]
end

function main()
    PATH = "./data/measurements_small.txt"
    my_file_size = filesize(PATH)
    @info "file size = $my_file_size"
    
    BUFFER_SIZE = 1<<24
    
    buffy = Vector{UInt8}(undef, BUFFER_SIZE)
    
    result_dict = Dict{UInt64, NTuple{4, Int32}}()

    open(PATH, "r") do file
        while true
            # either fill buffer or go to end of file, whichever is less
            bytes_to_read = min(BUFFER_SIZE, my_file_size - position(file))
            readbytes!(file, buffy, bytes_to_read)
            
            # restrict buffer to exclude a final partially read row
            my_copy = @view buffy[1:bytes_to_read]
            
            # this ugly condition checks for cut off final lines and is resistant to 3-5 char decimals and names with periods in them
            if !(my_copy[end-1] == PERIOD && (my_copy[end-3] == SEMICOL ||my_copy[end-4] == SEMICOL ||my_copy[end-5] == SEMICOL)) 
                last_newline = findlast(x->x==NEWLINE, my_copy)
                my_copy = @view my_copy[1: last_newline]
            
                # edit the stream cursor
                to_cut_off = BUFFER_SIZE - last_newline
                skip(file, -to_cut_off)
            end
            #try
            process_chunk!(result_dict, my_copy)
            
            
            #catch
            #    println(String(my_copy))
            #    break
            #end
            

            if bytes_to_read < BUFFER_SIZE || position(file) == my_file_size
                break
            end
        end
    end
    result_dict
    
end

@time d = main()

df = DataFrame(CSV.File("data/measurements_small.txt", header=["name", "x"]))
maxima = combine(groupby(df, [:name]), :x => maximum)
max_dict = Dict(map(kv -> (hash(kv[1]), Int(kv[2]*10)), zip(maxima.name, maxima.x_maximum)))

output_max = Dict(k => v[2] for (k,v) in d)
@assert max_dict==output_max