# like base dict with string keys but less allocs and GC time

using CSV, DataFrames, StringViews

const NEWLINE =  "\n"[1] |> UInt8
const SEMICOL =  ";"[1] |> UInt8
const PERIOD = "."[1] |> UInt8
const DASH = "-"[1] |> UInt8
const CARRIAGE = "\r"[1] |> UInt8
const SPACE = " "[1] |> UInt8
const TAB = "\t"[1] |> UInt8

names = CSV.File("data/weather_stations_clean.csv", header=["name", "x"]).name


struct StringViewDict{K, V}
    digest_map :: Dict{K, Vector{Tuple{String, Int}}}
    values :: Vector{V}
end

# key K is a digest of String key pointing to value V 
StringViewDict{K, V}() where{K,V} = StringViewDict{K, V}(Dict{K, V}(), Vector{V}())

import Base: setindex!
import Base: getindex
import Base: keys
import Base: haskey


function setindex!(d::StringViewDict{K, V}, value :: V, key :: StringView) :: V where {K, V}
    digest = hash(key)
    if haskey(d.digest_map, digest)
        
        write_index = -1
        for (full_key, _write_index) in d.digest_map[digest]
            # TODO make equality check more efficient
            if full_key == key
                write_index = _write_index
                break
            end
        end
        
        if write_index < 0
            # old digest new key (collision!)
            write_index = length(d.values) + 1
            push!(d.digest_map[digest], (String(key), write_index))
            push!(d.values, value)
        else
            # old digest old key
            @inbounds d.values[write_index] = value
        end
    else
        # new digest and therefore new key
        d.digest_map[digest] = Vector{Tuple{String, Int}}()
        write_index = length(d.values) + 1
        push!(d.digest_map[digest], (String(key), write_index))
        push!(d.values, value)
    end
    return value
end


function getindex(d::StringViewDict{K, V}, key :: StringView) :: V where {K, V}
    digest = hash(key)
    if ! haskey(d.digest_map, digest)
        @info "early"
        throw(KeyError(key))
    end
    for (full_key, value_index) in d.digest_map[digest]
        # TODO comparison
        if full_key == key
            @inbounds return d.values[value_index]
        end
    end
    @info "late"
    throw(KeyError(key))
end

function haskey(d::StringViewDict, key :: StringView)
    digest = hash(key)
    if ! haskey(d.digest_map, digest)
        return false
    end
    for (full_key, value_index) in d.digest_map[digest]
        # TODO comparison
        if full_key == key
            return true
        end
    end
    return false
end

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

function process_chunk!(result_dict::StringViewDict{UInt64, Tuple{Int32, Int32, Int32, Int32}}, buffer)
    rows = 0
    bytes_read = 1

    while bytes_read + 1 < length(buffer)
        
        sc = findnext(x -> x==SEMICOL, buffer, bytes_read)
        v = @view buffer[bytes_read:sc-1]
        key = StringView(v)
        bytes_read = sc + 1

        reading, bytes_read = parse_decimal(buffer, bytes_read)
        
        reading = Int32(reading)
       
        if haskey(result_dict, key)
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
    PATH = "/Users/alex/dev/1brc_data/measurements_small.txt"
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
        result_dict = StringViewDict{UInt64, NTuple{4, Int32}}()

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
            #@info @report_opt f()
        end
        results[thread_id] = result_dict
    end
    out = results[1]

    out
end

d = main()
@time main()
