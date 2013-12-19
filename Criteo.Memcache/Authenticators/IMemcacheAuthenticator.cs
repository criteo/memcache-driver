namespace Criteo.Memcache.Authenticators
{
    public interface IMemcacheAuthenticator
    {
        /// <summary>
        /// Create an authentication token for a single connection
        /// </summary>
        /// <returns>A new token for authenticating the connection</returns>
        IAuthenticationToken CreateToken();
    }
}
