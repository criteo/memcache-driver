namespace Criteo.Memcache
{
    /// <summary>
    /// The callback policy for redundant requests (ADD/SET/ADD/REPLACE/DELETE).
    /// </summary>
    public enum CallBackPolicy
    {
        /// <summary>
        /// Call the callback with an OK status as soon as the first Status.NoError response is received, or with
        /// the last failed status if all responses are fails.
        /// </summary>
        AnyOK,

        /// <summary>
        /// Call the callback with an OK status if all responses are OK, or with the first received failed status
        /// </summary>
        AllOK,
    };
}