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
  case class FeatGroup(W: Set[String], Q: Set[String], C: Set[String]) {
    override def toString = f"Group(W=[${W.toArray.sorted.mkString(",")}], Q=[${Q.toArray.sorted.mkString(",")}], C=[${C.toArray.sorted.mkString(",")}])"
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

    def getInitGraph(): GraphResultWrapper = {
      val knowledge = tetradRunner.createKnowledge()

      knowledge.addVariable("treatment")
      knowledge.addVariable("outcome")
      feats.foreach(f => knowledge.addVariable(f))

      knowledge.setRequired("treatment", "outcome")
      knowledge.setForbidden("outcome", "treatment")

      val result = GraphResultWrapper(runAlgorithm(cmdArgs, data, knowledge), FeatGroup(Set(), Set(), Set()), "init_search")
      println(f"${result.desc}, ${result.score}")
      result
    }

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

      add_forbid_cross(target_vals, group.W)
      add_forbid_cross(group.W, group.C)

      add_forbid_cross(target_vals, group.Q)
      add_forbid_cross(group.Q, target_vals)
      add_forbid_cross(group.Q, group.W ++ group.C)

      add_forbid_cross(group.C, target_vals)
      add_forbid_cross(group.C, group.W ++ group.Q)

      val result = GraphResultWrapper(runAlgorithm(cmdArgs, data, knowledge), group, log_desc)
      cache.putIfAbsent(group, result)
      // println(f"${result.score}, ${log_desc}, ${group}")
      result
    }

    def search(start_state: GraphResultWrapper): GraphResultWrapper = {
      var base: GraphResultWrapper = start_state
      var state = start_state.featGroup
      println(f"${base.desc}, ${base.score} ${base.featGroup}")

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
              getStateGraph(FeatGroup(w2, state.Q ++ Set(feat), state.C), s"${feat}->Q"),
              getStateGraph(FeatGroup(w2, state.Q, state.C ++ Set(feat)), s"${feat}->C"),
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
              getStateGraph(FeatGroup(state.W ++ Set(feat), state.Q, c2), s"${feat}->W"),
              getStateGraph(FeatGroup(state.W, state.Q ++ Set(feat), c2), s"${feat}->Q"),
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
              getStateGraph(FeatGroup(state.W ++ Set(feat), q2, state.C), s"${feat}->W"),
              getStateGraph(FeatGroup(state.W, q2, state.C ++ Set(feat)), s"${feat}->C"),
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

    val init_graph = getInitGraph()
    println("init graph:")
    println(init_graph.format_graph_txt)

    val init_edges = init_graph.graph.getEdges.asScala.toArray
    val init_c = init_edges
      .filter(e => (Seq("treatment", "outcome").contains(e.getNode1.getName)))
      .map(_.getNode1.getName)
      .filterNot(Seq("treatment", "outcome").contains)
      .toSet
    val init_w = feats.toSet.diff(init_c).toArray.toSet
    val init_state_base = FeatGroup(W=init_w, Q=Set(), C=init_c)
    val init_q = Set[String]()

    val search_start_states = new ArrayBuffer[FeatGroup]()
    search_start_states += init_state_base

    val random = new Random()

    for(_ <- 1 until 10) {
      val w_buffer = new ArrayBuffer[String]()
      val q_buffer = new ArrayBuffer[String]()
      val c_buffer = new ArrayBuffer[String]()

      for (f <- feats) {
        if(random.nextFloat() < 0.8) {
          if(init_w.contains(f)) {
            w_buffer += f
          } else if(init_q.contains(f)) {
            q_buffer += f
          } else if(init_c.contains(f)) {
            c_buffer += f
          }
        } else {
          val target = Seq(q_buffer, w_buffer, c_buffer)(random.nextInt(3))
          target += f
        }
      }
      val state = FeatGroup(W = w_buffer.toSet, Q = q_buffer.toSet, C = c_buffer.toSet)
      search_start_states += state
    }


    val search_result_list = search_start_states.zipWithIndex
      .map{case (state, run_idx) =>
        val state_graph = getStateGraph(state, f"Random_Run${run_idx}%02d")
        val result = search(state_graph).copy(desc = f"Run${run_idx}%02d_Result")
        println(f"${result.desc}, ${result.score} ${result.featGroup}")
        result
      }
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
