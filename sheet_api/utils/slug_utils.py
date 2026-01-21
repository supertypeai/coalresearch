"""
Utility functions for generating URL-friendly slugs from company names.
"""


def generate_slug(name: str) -> str:
    """
    Generate a slug from a company name by converting to lowercase
    and replacing spaces with hyphens.

    Args:
        name (str): The company name to convert

    Returns:
        str: The slug version of the name

    Examples:
        >>> generate_slug("PT Adaro Andalan Indonesia Tbk")
        'pt-adaro-andalan-indonesia-tbk'
        >>> generate_slug("PT ABM Investama Tbk")
        'pt-abm-investama-tbk'
        >>> generate_slug("PT Artha Mahiya Investama Tbk")
        'pt-artha-mahiya-investama-tbk'
    """
    if not name:
        return ""

    # Convert to lowercase and replace spaces with hyphens
    slug = name.lower().replace(" ", "-")

    return slug
