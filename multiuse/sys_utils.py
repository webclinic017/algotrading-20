"""System utility functions."""
# %% codecell
import psutil

# %% codecell


def get_memory_usage():
    """Get RAM and SWAP usage."""
    # If regular memory usage > 95% (.95)
    vmp = psutil.virtual_memory()[2]
    # And SWAP usage is > 50% (.50)
    swapp = psutil.swap_memory()[3]
    # THRESHOLD = 3000 * 1024 * 1024  # 3GB
    # Then write all data to local file. Then delete other files
    if (vmp > .95) or (swapp > .50):
        return False
    else:
        return True

# %% codecell
