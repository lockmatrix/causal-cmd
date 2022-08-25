package edu.pitt.dbmi.causal.cmd

import edu.cmu.tetrad.data.{DataModel, IKnowledge}
import edu.cmu.tetrad.graph.Graph
import edu.pitt.dbmi.causal.cmd.tetrad.TetradRunner

import java.io.{BufferedOutputStream, PrintStream}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util
import java.util.concurrent.{ConcurrentHashMap, ForkJoinPool}
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.CollectionConverters.ArrayIsParallelizable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Random

object Main {
  case class FeatGroup(W: Set[String], Q: Set[String], C: Set[String], A: Set[String]) {
    override def toString = f"Group(W=[${W.mkString(",")}], Q=[${Q.mkString(",")}], C=[${C.mkString(",")}], A=[${A.mkString(",")}])"
  }

  case class GraphResultWrapper(graph: Graph, featGroup: FeatGroup, desc: String) {
    def score: Double = graph.getAttribute("BIC").asInstanceOf[java.lang.Double].doubleValue()

    def format_graph_txt: String = graph.toString
  }

  def main(args: Array[String]): Unit = {
    val cmdArgs = CmdParser.parse(args)

    val outDir = cmdArgs.getOutDirectory.toString
    val prefix = cmdArgs.getFilePrefix
    val outTxtFile = Paths.get(outDir, String.format("%s.txt", prefix))

    val out = new PrintStream(new BufferedOutputStream(Files.newOutputStream(outTxtFile, StandardOpenOption.CREATE)))

    val thread_num = 5
    println(f"thread_num: ${thread_num}")
    val thread_pool = new ForkJoinPool(thread_num)

    val tetradRunner = new TetradRunner(cmdArgs)
    val data = tetradRunner.loadData(out)

    val feats = data.get(0).getVariableNames.asScala.toArray.filter(_.startsWith("V"))

    val cache = new ConcurrentHashMap[FeatGroup, GraphResultWrapper]()

    def getStateGraph(group: FeatGroup, log_desc: String): GraphResultWrapper = {
      val cached_result = cache.get(group)
      if(cached_result != null) {
        return cached_result
      }

      val knowledge = tetradRunner.createKnowledge()

      knowledge.addVariable("treatment")
      knowledge.addVariable("outcome")
      feats.foreach(f=> knowledge.addVariable(f))

      knowledge.setRequired("treatment", "outcome")
      knowledge.setForbidden("outcome", "treatment")

      val target_vals = Set("treatment", "outcome")

      def add_forbid_inner(list: Set[String]): Unit = {
        for(
          i <- list;
          j <- list;
          if i != j
        ) {
          knowledge.setForbidden(i, j)
        }
      }

      def add_forbid_cross(l1: Set[String], l2: Set[String]): Unit = {
        for (
          i <- l1;
          j <- l2;
          if i != j
        ) {
          knowledge.setForbidden(i, j)
        }
      }

      add_forbid_inner(group.W)
      add_forbid_cross(target_vals, group.W)
      add_forbid_cross(group.W, group.C ++ group.A)

      add_forbid_inner(group.Q)
      add_forbid_cross(target_vals, group.Q)
      add_forbid_cross(group.Q, target_vals)
      add_forbid_cross(group.Q, group.W ++ group.C ++ group.A)

      add_forbid_inner(group.C)
      add_forbid_cross(group.C, target_vals)
      add_forbid_cross(group.C, group.W ++ group.Q ++ group.A)

      add_forbid_inner(group.A)
      add_forbid_cross(target_vals, group.A)
      add_forbid_cross(group.A, target_vals)
      add_forbid_cross(group.A, group.W ++ group.C ++ group.Q)

      val result = GraphResultWrapper(runAlgorithm(cmdArgs, data, knowledge), group, log_desc)
      cache.putIfAbsent(group, result)
      // println(f"${result.score}, ${log_desc}, ${group}")
      result
    }

    def search(start_state: GraphResultWrapper): GraphResultWrapper = {
      var base: GraphResultWrapper = start_state
      var state = start_state.featGroup
      println(f"${base.desc}, ${base.score} ${base.featGroup}")

      def tryFromA(): Boolean = {
        println("tryFromA...")

        var changed = false
        while (true) {
          val job_par = state.A.toArray
            .par

          if(job_par.isEmpty) {
            return changed
          }

          job_par.tasksupport = new ForkJoinTaskSupport(thread_pool)

          val maxResult = job_par.flatMap(feat => {
            val a2 = state.A.filter(_ != feat)
            Seq(
              getStateGraph(FeatGroup(state.W ++ Set(feat), state.Q, state.C, a2), s"${feat}->W"),
              getStateGraph(FeatGroup(state.W, state.Q ++ Set(feat), state.C, a2), s"${feat}->Q"),
              getStateGraph(FeatGroup(state.W, state.Q, state.C ++ Set(feat), a2), s"${feat}->C")
            )
          }).toArray
            .maxBy(_.score)

          if (maxResult.score > base.score) {
            base = maxResult
            state = maxResult.featGroup
            println(f"Update ${base.desc}, ${base.score} ${base.featGroup}")
            changed = true
          }
          else {
            return changed
          }
        }
        return changed
      }

      def tryFromW(): Boolean = {
        println("tryFromW...")

        var changed = false
        while (true) {
          val job_par = state.W.toArray
            .par

          if (job_par.isEmpty) {
            return changed
          }

          job_par.tasksupport = new ForkJoinTaskSupport(thread_pool)

          val maxResult = job_par.flatMap(feat => {
            val w2 = state.W.filter(_ != feat)
            Seq(
              getStateGraph(FeatGroup(w2, state.Q ++ Set(feat), state.C, state.A), s"${feat}->Q"),
              getStateGraph(FeatGroup(w2, state.Q, state.C ++ Set(feat), state.A), s"${feat}->C"),
              getStateGraph(FeatGroup(w2, state.Q, state.C, state.A ++ Set(feat)), s"${feat}->A"),
            )
          }).toArray
            .maxBy(_.score)

          if (maxResult.score > base.score) {
            base = maxResult
            state = maxResult.featGroup
            println(f"Update ${base.desc}, ${base.score} ${base.featGroup}")
            changed = true
          }
          else {
            return changed
          }
        }
        return changed
      }

      def tryFromC(): Boolean = {
        println("tryFromC...")

        var changed = false
        while (true) {
          val job_par = state.C.toArray
            .par

          if (job_par.isEmpty) {
            return changed
          }

          job_par.tasksupport = new ForkJoinTaskSupport(thread_pool)

          val maxResult = job_par.flatMap(feat => {
            val c2 = state.C.filter(_ != feat)
            Seq(
              getStateGraph(FeatGroup(state.W ++ Set(feat), state.Q, c2, state.A), s"${feat}->W"),
              getStateGraph(FeatGroup(state.W, state.Q ++ Set(feat), c2, state.A), s"${feat}->Q"),
              getStateGraph(FeatGroup(state.W, state.Q, c2, state.A ++ Set(feat)), s"${feat}->A"),
            )
          }).toArray
            .maxBy(_.score)

          if (maxResult.score > base.score) {
            base = maxResult
            state = maxResult.featGroup
            println(f"Update ${base.desc}, ${base.score} ${base.featGroup}")
            changed = true
          }
          else {
            return changed
          }
        }
        return changed
      }

      def tryFromQ(): Boolean = {
        println("tryFromQ...")

        var changed = false
        while (true) {
          val job_par = state.Q.toArray
            .par

          if (job_par.isEmpty) {
            return changed
          }

          job_par.tasksupport = new ForkJoinTaskSupport(thread_pool)

          val maxResult = job_par.flatMap(feat => {
            val q2 = state.Q.filter(_ != feat)
            Seq(
              getStateGraph(FeatGroup(state.W ++ Set(feat), q2, state.C, state.A), s"${feat}->W"),
              getStateGraph(FeatGroup(state.W, q2, state.C ++ Set(feat), state.A), s"${feat}->C"),
              getStateGraph(FeatGroup(state.W, q2, state.C, state.A ++ Set(feat)), s"${feat}->A"),
            )
          }).toArray
            .maxBy(_.score)

          if (maxResult.score > base.score) {
            base = maxResult
            state = maxResult.featGroup
            println(f"Update ${base.desc}, ${base.score} ${base.featGroup}")
            changed = true
          }
          else {
            return changed
          }
        }
        return changed
      }

      val action_list = Seq(
        ()=> tryFromA(),
        () => tryFromQ(),
        () => tryFromW(),
        () => tryFromC(),
      )

      while(true) {
        var changed = false
        action_list.foreach(a=>{
          val ret = a()
          if(ret) {
            changed = true
          }
        })
        if(!changed) {
          return base
        }
      }

      base
    }

    val search_result_list = (0 until 10)
      .map(run_idx =>{
        val state = {
          val w_buffer = new ArrayBuffer[String]()
          val q_buffer = new ArrayBuffer[String]()
          val c_buffer = new ArrayBuffer[String]()
          val a_buffer = new ArrayBuffer[String]()

          val random = new Random()
          for (f <- feats) {
            val target = Seq(q_buffer, w_buffer, c_buffer, a_buffer)(random.nextInt(4))
            target += f
          }
          FeatGroup(W = w_buffer.toSet, Q = q_buffer.toSet, C = c_buffer.toSet, A = a_buffer.toSet)
        }
        val state_graph = getStateGraph(state, f"Random_Run${run_idx}%02d")
        val result = search(state_graph).copy(desc = f"Run${run_idx}%02d_Result")
        println(f"${result.desc}, ${result.score} ${result.featGroup}")
        result
      })
      .sortBy(-_.score)

    println("multi restart results:")
    search_result_list.foreach(g=>{
      println(f"${g.desc}, ${g.score} ${g.featGroup}")
    })

    val search_result = search_result_list.head
    println(search_result.score)
    println(search_result.featGroup)
    println(search_result.format_graph_txt)

    out.println("=================================")
    out.println(search_result.format_graph_txt)

    out.close()
  }

  def runAlgorithm(cmdArgs: CmdArgs, data: util.List[DataModel], knowledge: IKnowledge): Graph = {
    val tetradRunner = new TetradRunner(cmdArgs)
    tetradRunner.runAlgorithmRaw(data, knowledge)

    val graph = tetradRunner.getGraphs.get(0)
    graph
  }
}
