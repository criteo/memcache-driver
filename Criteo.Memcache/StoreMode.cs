namespace Criteo.Memcache
{
    public enum StoreMode
    {
        /// <summary>
        /// Creates or replace an already existing value
        /// </summary>
        Set,

        /// <summary>
        /// Fails with status KeyNotFound if not present
        /// </summary>
        Replace,

        /// <summary>
        /// Fails with status KeyExists if already present
        /// </summary>
        Add,
    }
}