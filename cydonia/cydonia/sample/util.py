""" Function mask_equiv_bit and mask_equiv_bits by Carl Waldspurger. 
    Code taken from a screenshot in email sent by
    Carl Waldspurger on (Sat, 19 Jun 2021 12:49:49 -0700) with 
    subject "non-contiguous locality sampling". 
"""
def clear_bit(value, bit):
    return value & ~(1<<bit)


def mask_equiv_bits(addr, bits):
    """ Return addr when ignoring multiple bits. 
        
        Parameters
        ----------
        addr (int) : the address (could be Logical Block Address)
        bits (list) : list of bits to ignore

        Return 
        ------
        new_addr (int) : the address equivalent to "addr" while ignoring specified bits
    """
    if bits is None:
        return addr 
    else:
        new_addr = addr
        for b in bits:
            new_addr = clear_bit(new_addr, b)
        return new_addr


def ignore_n_low_order_bits(addr, n):
    """ Return addr when ignoring n lower order bits. 
        
        Parameters
        ----------
        addr (int) : the address (could be Logical Block Address)
        n (int) : the number of lower bits to ignore 

        Return 
        ------
        new_addr (int) : the address equivalent to "addr" while ignoring specified bits
    """
    new_addr = addr 
    for i in range(n):
        new_addr = clear_bit(new_addr, i)
    return new_addr