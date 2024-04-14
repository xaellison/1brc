# this file shows how to read a file in byte chunks but the chunks will have broken lines

function main()
    PATH = "./data/measurements_small.txt"
    my_file_size = filesize(PATH)
    @info "file size = $my_file_size"
    all_bytes = String(read(open(PATH, "r")))

    BUFFER_SIZE = 1<<8
    buffy = Vector{UInt8}(undef, BUFFER_SIZE)

    copies = []
    open(PATH, "r") do file
        while true
            bytes_to_read = min(BUFFER_SIZE, my_file_size - position(file))
            readbytes!(file, buffy, bytes_to_read)
            
            my_copy = @view buffy[1:bytes_to_read]
            push!(copies, String(my_copy))
            
            if bytes_to_read < BUFFER_SIZE || position(file) == my_file_size
                break
            end
        end
    end
    @assert join(copies) == all_bytes

end


@time main()