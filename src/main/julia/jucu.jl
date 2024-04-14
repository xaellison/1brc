

#using CSV, DataStructures


const NEWLINE =  "\n"[1] |> UInt8
const SEMICOL =  ";"[1] |> UInt8
const PERIOD = "."[1] |> UInt8
const DASH = "-"[1] |> UInt8
const CARRIAGE = "\r"[1] |> UInt8
const SPACE = " "[1] |> UInt8
const TAB = "\t"[1] |> UInt8

function identify_cities()

    block_size = 1<<15
    open("C:/Users/sandy/Documents/Github/1brc/data/measurements_small.txt") do file
        # Continue reading until the end of the file
        while !eof(file)
            # Initialize a list to hold the lines
            lines = []
            # Read up to 10,000 lines
            for i in 1:block_size
                if eof(file)
                    break
                end
                @time push!(lines, readline(file))
            end

            name_to_id = Dict{String, Int}()
            id_to_name = Dict{Int, String}()
            current_city_id = 0

            for line in lines
                split = findfirst(';', line)
                name = @view(line[1:prevind(line, split)])
                id = Int32(Int(hash(name)) & typemax(Int32))
                measurement = parse(Float32, @view(line[split+1:end]))

                if !(name in keys(name_to_id))
                    id_to_name[current_city_id] = name
                    name_to_id[name] = current_city_id
                    current_city_id += 1
                end

            end
        end
    end


end


function simple_iter()

    block_size = 1<<15
    open("data/measurements_small.txt") do file
        # Continue reading until the end of the file
        while !eof(file)
            # Initialize a list to hold the lines
            lines = []
            # Read up to 10,000 lines
            for i in 1:block_size
                if eof(file)
                    break
                end
                push!(lines, readline(file))
            end
            
            block = Array{Tuple{UInt32, Float32}}(undef, block_size)

            for (n, line) in enumerate(lines)
                split = findfirst(';', line)
                name = @view(line[1:prevind(line, split)])
                h = hash(name)
                u64 = UInt64(h)
                trunc_u64 = u64 & typemax(UInt32)
                u32 = UInt32(trunc_u64)
                measurement = parse(Float32, @view(line[split+1:end]))
                block[n] = u32, measurement
            end
        end
    end
end

function csv_iter()


    # single thread
    rows_per_city = counter(String)
    name_to_id = Dict{String, Int}()
    id_to_name = Dict{Int, String}()
    current_city_id = 0
    for row in CSV.File("C:/Users/sandy/Documents/Github/1brc/data/measurements_small.txt", header=["name", "measurement"] )
        inc!(rows_per_city, row.name)
        if !(row.name in keys(name_to_id))
            id_to_name[current_city_id] = row.name
            name_to_id[row.name] = current_city_id
            current_city_id += 1
        end
    end

end

function munch_name(buffy, i)
    # creates a shitty hash of the name encoded starting at buffy[i] until the next semi colon from position i 
    # returns hash and index to start parsing number at
    word1 = 0xff
    word2 = 0xff
    word3 = 0xff
    word4 = 0xff
    i0 = i
    #@info "buffer here to end: $(String(buffy[i:end]))"

    while buffy[i] != SEMICOL
        if i % 4 == 1
            word1 = word1 ⊻ buffy[i]
        end
        if i % 4 == 2
            word2 = word2 ⊻ buffy[i]
        end
        if i % 4 == 3
            word3 = word3 ⊻ buffy[i]
        end
        if i % 4 == 0
            word4 = word4 ⊻ buffy[i]
        end
        i += 1
    end
 #   @info "Parsed out name = $(String(buffy[i0:i]))"
    return UInt32(word4) << 24 + UInt32(word3) << 16 + UInt32(word2) << 8 + UInt32(word1), i + 1
end

function munch_decimal(buffy, i)
    # non-zero padded decimal from -99.9 to 99.9 always with one decimal place. Treat as int from -999 to 999
  #  @info "decimal start, rest of buffer: $(String(buffy[i:end]))"
    if buffy[i] == DASH
        sign = -one(Int32)
        i += 1
    else
        sign = one(Int32)
    end
    value = Int32(buffy[i] - 0x30) * 100 + Int32(buffy[i+1] - 0x30) * 10 + Int32(buffy[i+3] - 0x30)
  #  @info "Parsed out number $(value)"
    #@info "$(String(buffy[i:end]))" 
    #@info "$(buffy[i:end])" 
    i += 4
    
    while (buffy[i] == SPACE || buffy[i] == TAB || buffy[i] == NEWLINE || buffy[i] == CARRIAGE)
        i += 1
    end
    
    # lines appear to end in \r then \n, so if [i+3] is the tenths place of measurement, +6 is the first char of next line
    return value * sign, i

end

function raw_read_chunks()
    total = 0
    # max bytes in a row is ~110 (100 for name, <10 for ;-99.9\n)
    MAX_ROW_SIZE = 110
    BUFFER_SIZE = 1<<20
    buffy = Vector{UInt8}(undef, BUFFER_SIZE)
    open("./data/measurements.txt", "r") do file
        # Continue reading until the end of the file
        #while !eof(file)
        total_bytes_read = 0
        while !eof(file)
           # @warn "reload buffer!"
            
            read!(file, buffy)

           # @info String(buffy[1:end])

            bytes_read = 1
            while MAX_ROW_SIZE < BUFFER_SIZE - bytes_read
                name_digest, bytes_read = munch_name(buffy, bytes_read)
                reading, bytes_read = munch_decimal(buffy, bytes_read)
                total+=1
                total_bytes_read += bytes_read - 1
            end 
            seek(file, total_bytes_read)
        
        end
        #end
    end
    @info total
end


function raw_read()
    
    # max bytes in a row is ~110 (100 for name, <10 for ;-99.9\n)
    MAX_ROW_SIZE = 110
    read_bytes = 1<<7
    buffy = Vector{UInt8}(undef, read_bytes)
    open("./data/measurements_nano.txt", "r") do file
        # Continue reading until the end of the file
        #while !eof(file)
        total_bytes_read = 0
        while !eof(file)
            #@warn "seek!"
            
            read!(file, buffy)
            name_digest, bytes_read = munch_name(buffy, 1)
            reading, bytes_read = munch_decimal(buffy, bytes_read)
            total_bytes_read += bytes_read - 1
            seek(file, total_bytes_read)
        
        end
        #end
    end

end

raw_read_chunks()