(port == "443" and "https://") or "http://"
#x and y returns y if x is truish, x if x is falsish; a or b, vice versa, returns a if it's truish, otherwise b.
#So if port == "443" is true, this returns the RHS of the and, i.e., "https://". Otherwise, the and is false, so the or gets into play and returns `"http://", its RHS.
#In modern Python, a better way to do translate this old-ish idiom is:"

"https://" if port == "443" else "http://"


#all these are examples, idk if they really work...
def protocol(port):
    if port == "443":
        if bool("https://"):
            return True
    elif bool("http://"):
        return True
    return False

    def protocol(port):
    if port == "443":
        return True + "https://"
    else:
        return True + "http://"