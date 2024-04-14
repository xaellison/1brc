@inline function extract_decimal(buffy, i=1)

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
        return value * sign, i
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
    end
#    i+=1
    return value * sign, i
end