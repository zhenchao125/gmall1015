/**
 * Author atguigu
 * Date 2020/4/2 10:09
 */
object Test1 {
    def main(args: Array[String]): Unit = {
        foo()
        
        println("xxxxxxx")
    }
    
    def foo(): Unit = {
        // 如何提前退出匿名函数
        try{
            List(1, 2, 3).foreach(x => {
                println(x)
                if (x == 2) return
            })
        }catch {
            case _ =>
        }
        
        println("foo....")
    }
}
