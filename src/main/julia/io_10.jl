# multithreading verified parsing and agg for maximum against DataFrames.jl. Uses small dataset so not all values are 99.9

using CSV, DataFrames, StringViews

const NEWLINE =  "\n"[1] |> UInt8
const SEMICOL =  ";"[1] |> UInt8
const PERIOD = "."[1] |> UInt8
const DASH = "-"[1] |> UInt8
const CARRIAGE = "\r"[1] |> UInt8
const SPACE = " "[1] |> UInt8
const TAB = "\t"[1] |> UInt8

names = CSV.File("data/weather_stations_clean.csv", header=["name", "x"]).name


# for testing
function name_digest(str, p1=11, p2=53)
    parse_name_digest(Vector{UInt8}("$(str);"), 1, p1, p2)[1]
end


@inline function parse_name_digest(buffer, i, p1=11, p2=53)
    # p1 and p2 are params that adjust the hashing
    @inbounds begin
        # creates a shitty hash of the name encoded starting at buffer[i] until the next semi colon from position i 
        # returns hash and index to start parsing number at
        
        if buffer[i] == NEWLINE
            i += 1
        end

        i0 = i
        #@info "buffer here to end: $(String(buffer[i:end]))"
        out = zero(UInt64)
        while buffer[i] != SEMICOL
            if i - i0 < 8 
                shift_bits = 8 * (i - i0)
            else
                shift_bits = ((i - i0) * p1) % p2
            end
            out = out ⊻ (UInt64(buffer[i]) << shift_bits)
            i += 1
        end
        
        return i0, i - 1, i + 1
    end
end

@inline function parse_decimal(buffer, i)
    @inbounds begin
    # non-zero padded decimal from -99.9 to 99.9 always with one decimal place. Treat as int from -999 to 999
  #  @info "decimal start, rest of buffer: $(String(buffer[i:end]))"
    
    
     # get +/-
    if buffer[i] == DASH
        sign = -one(Int32)
        i += 1
    else
        sign = one(Int32)
    end

    # leading digit
    value = Int32(buffer[i] - 0x30)
    i+=1
    
    if buffer[i] == PERIOD
        # if next char is '.' then abs this number < 10
        i += 1
        value *= 10
        value += Int32(buffer[i] - 0x30)
        i += 1
    else
        # abs this number >= 10
        value *= 10
        # add next leading decimal
        value += Int32(buffer[i] - 0x30)
        
        # add two because next char guaranteed to be '.'
        i+=2

        # add final decimal place
        value *= 10
        value += Int32(buffer[i] - 0x30)
        i += 1
    end

    
    while (buffer[i] == SPACE || buffer[i] == TAB || buffer[i] == NEWLINE || buffer[i] == CARRIAGE)
        i += 1
    end
    # lines appear to end in \r then \n, so if [i+3] is the tenths place of measurement, +6 is the first char of next line
    return value * sign, i
    end
end

function process_chunk!(result_dict, buffer)
    rows = 0
    bytes_read = 1

    while bytes_read + 1 < length(buffer)
        
        string_start, string_end, bytes_read = parse_name_digest(buffer, bytes_read)
        if bytes_read + 1 >= length(buffer)
            break
        end
        reading, bytes_read = parse_decimal(buffer, bytes_read)
        
        reading = Int32(reading)
        
        key = StringView(view(buffer, string_start:string_end))
        
        if haskey(result_dict, key)
            key = String(key)
            V = result_dict[key]
            V = merge_values(V, (reading, reading, reading, Int32(1)))
            result_dict[key] = V
        else
            V = typemax(Int32), typemin(Int32), zero(Int32), zero(Int32)
            V = merge_values(V, (reading, reading, reading, Int32(1)))
            result_dict[key] = V
        end
        
    end
end



function merge_values(v1 :: NTuple{4, T}, v2 :: NTuple{4, T}) :: NTuple{4, T} where T
    min(v1[1], v2[1]), max(v1[2], v2[2]), v1[3] + v2[3], v1[4] + v2[4]
end


function split_into_chunks(path, chunks; delimiter=NEWLINE, max_line_length=256)
    out = [0]
    file_size = filesize(path)
    
    open(path, "r") do file
        buffer = Vector{UInt8}(undef, max_line_length)
        
        for i in 1:chunks-1
            seek_to_position = file_size * i ÷ chunks
            seek(file, seek_to_position)
            readbytes!(file, buffer, max_line_length)
            last_delimiter_in_buffer = findlast(x->x==delimiter, buffer)
            @assert ! isnothing(last_delimiter_in_buffer)
            
            last_delimiter_in_file = last_delimiter_in_buffer + (seek_to_position - 1)
            seek(file, last_delimiter_in_file)
            
            @assert peek(file) == delimiter
            push!(out, last_delimiter_in_file)
        end
    end
    push!(out, file_size)
    out
end


function main()
    PATH = "./data/measurements_mid.txt"
    my_file_size = filesize(PATH)
    @info "file size = $my_file_size"
    
    BUFFER_SIZE = 1<<22
    # avoids bug  SystemError: skip: Invalid argument
    while BUFFER_SIZE > my_file_size
        BUFFER_SIZE = BUFFER_SIZE >> 1
    end
    
    workers = Threads.nthreads() * 2
    thread_boundaries = split_into_chunks(PATH, workers)

    results = Array{Any}(undef, workers)

     for thread_id in 1:workers
        thread_start = thread_boundaries[thread_id] + 1
        thread_end = thread_boundaries[thread_id + 1]
        
        buffer = Vector{UInt8}(undef, BUFFER_SIZE)
        result_dict = Dict{Any, NTuple{4, Int32}}()

        open(PATH, "r") do file
            skip(file, thread_start - 1)
            while true
                # either fill buffer or go to end of file, whichever is less
                bytes_to_read = min(BUFFER_SIZE, thread_end - position(file))
                readbytes!(file, buffer, bytes_to_read)
            
                # restrict buffer to exclude a final partially read row
                my_copy = @view buffer[1:bytes_to_read]
                if my_copy[end] != NEWLINE
                    last_newline = findlast(x->x==NEWLINE, my_copy)
                    @assert !isnothing(last_newline)
                    my_copy = @view my_copy[1: last_newline]
                
                    # edit the stream cursor
                    to_cut_off = BUFFER_SIZE - last_newline
                    skip(file, -to_cut_off)
                end
                
                process_chunk!(result_dict, my_copy)

                if bytes_to_read < BUFFER_SIZE || position(file) == my_file_size
                    break
                end
            end
        end
        results[thread_id] = result_dict
    end
    out = results[1]

    for i in 2:length(results)
        for k in keys(results[i])
            if k in keys(out)
                out[k] = merge_values(out[k], results[i][k])
            else
                out[k] = results[i][k]
            end
        end
    end
    out
end

@time d = main()

