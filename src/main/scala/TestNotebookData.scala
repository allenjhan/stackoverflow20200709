object TestNotebookData extends App{
  //parallel notebook code

  import scala.concurrent.{Future, Await}
  import scala.concurrent.duration._
  import scala.util.control.NonFatal

  case class NotebookData(path: String, timeout: Int, parameters: Map[String, String] = Map.empty[String, String])

  case class Context()

  case class Notebook(){
    def getContext(): Context = Context()
    def setContext(ctx: Context): Unit = ()
    def run(path: String, timeout: Int, paramters: Map[String, String] = Map()): Seq[String] = Seq()
  }
  case class Dbutils(notebook: Notebook)

  val dbutils = Dbutils(Notebook())


  def parallelNotebooks(notebooks: Seq[NotebookData]): Future[Seq[Seq[String]]] = {
    import scala.concurrent.{Future, blocking, Await}
    import java.util.concurrent.Executors
    import scala.concurrent.ExecutionContext

    val numNotebooksInParallel = 5
    // If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once.
    // This code limits the number of parallel notebooks.
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numNotebooksInParallel))
    val ctx = dbutils.notebook.getContext()

    val isRetryable = true
    val retries = 5

    def runNotebook(notebook: NotebookData): Future[Seq[String]] = {
      def retryWrapper(retry: Boolean, current: Int, max: Int): Future[Seq[String]] = {
        val fut = Future {runNotebookInner}
        if (retry && current < max) fut.recoverWith{ _ => retryWrapper(retry, current + 1, max)}
        else fut
      }

      def runNotebookInner() = {
        dbutils.notebook.setContext(ctx)
        if (notebook.parameters.nonEmpty)
          dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
        else
          dbutils.notebook.run(notebook.path, notebook.timeout)
      }

      retryWrapper(isRetryable, 0, retries)
    }


    Future.sequence(
      notebooks.map { notebook =>
        runNotebook(notebook)
      }
    )
  }

  val notebooks = Seq(
    NotebookData("Notebook1", 0, Map("client"->"client")),
    NotebookData("Notebook2", 0, Map("client"->"client"))
  )
  val res = parallelNotebooks(notebooks)
  Await.result(res, 3000000 seconds) // this is a blocking call.
  res.value
}
