# this file shows how to read a file in byte chunks such that each chunk
# has complete lines
 
const NEWLINE =  "\n"[1] |> UInt8
const PERIOD = "."[1] |> UInt8

function main()
    PATH = "./data/measurements_small.txt"
    my_file_size = filesize(PATH)
    @info "file size = $my_file_size"
    all_bytes = String(read(open(PATH, "r")))

    BUFFER_SIZE = 1<<7
    buffy = Vector{UInt8}(undef, BUFFER_SIZE)

    copies = []
    open(PATH, "r") do file
        while true
            # either fill buffer or go to end of file, whichever is less
            bytes_to_read = min(BUFFER_SIZE, my_file_size - position(file))
            readbytes!(file, buffy, bytes_to_read)
            
            # restrict buffer to exclude a final partially read row
            my_copy = @view buffy[1:bytes_to_read]
            
            if !(my_copy[end-1] == PERIOD && (my_copy[end-3] == SEMICOL ||my_copy[end-4] == SEMICOL ||my_copy[end-5] == SEMICOL)) 
                last_newline = findlast(x->x==NEWLINE, my_copy)
                my_copy = @view my_copy[1: last_newline]
            
                # edit the stream cursor
                to_cut_off = BUFFER_SIZE - last_newline
                skip(file, -to_cut_off)
            end
            push!(copies, String(my_copy))
            if bytes_to_read < BUFFER_SIZE || position(file) == my_file_size
                break
            end
        end
    end
    try
        @assert join(copies) == all_bytes
    catch
        @error "Failed"
        @info join(copies)
        @info all_bytes
    end
end


@time main()