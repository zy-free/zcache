# zcache
解决redis与mysql数据一致性问题，防止缓存击穿/穿透/雪崩

缓存穿透：设置了一个特殊符号数据

缓存穿透：通过单飞模式控制并发

缓存雪崩：设置不同的过期时间


目前支持sql与redis如下（未做抽象）：

"database/sql"
"github.com/gomodule/redigo/redis"

