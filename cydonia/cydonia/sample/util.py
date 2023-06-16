""" Function mask_equiv_bit and mask_equiv_bits by Carl Waldspurger. 
    Code taken from a screenshot in email sent by
    Carl Waldspurger on (Sat, 19 Jun 2021 12:49:49 -0700) with 
    subject "non-contiguous locality sampling". 
"""
def mask_equiv_bit(addr, bit):
    """ Return address equivalent to addr while ignoring specified bit.
        
        Parameters
        ----------
        addr (int) : the address (could be Logical Block Address)
        bit (int) : the bit to ignore 

        Return 
        ------
        new_addr (int) : the address equivalent to "addr" while ignoring specified bit 
    """
    mask = 1 << bit;
    if addr & mask:
        return addr & ~mask
    else:
        return addr | mask 


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
            new_addr = mask_equiv_bit(new_addr, b)
        return new_addr