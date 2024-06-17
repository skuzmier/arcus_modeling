"""singelton token manager for NRG Stream API
"""

from auth import NRGAuth

class TokenManager:
    """
    """
    
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        """
        if not TokenManager._instance:
            TokenManager._instance = super(TokenManager, cls).__new__(cls, *args, **kwargs)
        return TokenManager._instance