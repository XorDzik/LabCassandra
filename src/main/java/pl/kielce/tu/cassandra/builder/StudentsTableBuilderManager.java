package pl.kielce.tu.cassandra.builder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.ArithmeticRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Update;

import com.datastax.oss.driver.internal.querybuilder.term.ArithmeticTerm;
import pl.kielce.tu.cassandra.simple.SimpleManager;

import java.util.Random;
import java.util.Scanner;

public class StudentsTableBuilderManager extends SimpleManager {
	final private static Random r = new Random(System.currentTimeMillis());

	public StudentsTableBuilderManager(CqlSession session) {
		super(session);
	}

	public void createTable() {
		CreateTable createTable = SchemaBuilder.createTable("club")
				.withPartitionKey("id", DataTypes.INT)
				.withColumn("name", DataTypes.TEXT)
				.withColumn("points", DataTypes.INT)
				.withColumn("goals_scored", DataTypes.INT)
				.withColumn("goals_lost", DataTypes.INT)
				.withColumn("ranking", DataTypes.INT);
		session.execute(createTable.build());
	}

	public void addClub() {
		Long id = (long) Math.abs(r.nextInt());
		Scanner scanner = new Scanner(System.in);

		System.out.println("Podaj nazwę klubu");
		String name = scanner.nextLine();

		System.out.println("Podaj liczbę punktów klubu");
		String points = scanner.nextLine();

		System.out.println("Podaj ilość strzelonych bramek");
		String goalsScored = scanner.nextLine();

		System.out.println("Podaj ilość straconych bramek");
		String goalsLost = scanner.nextLine();

		System.out.println("Podaj pozycję klubu w lidze");
		String ranking = scanner.nextLine();

		Insert insert = QueryBuilder.insertInto("sports_league", "club")
				.value("id", QueryBuilder.raw(String.valueOf(id)))
				.value("name", QueryBuilder.raw("'" + name + "'"))
				.value("points", QueryBuilder.raw(points))
				.value("goals_scored", QueryBuilder.raw(goalsScored))
				.value("goals_lost", QueryBuilder.raw(goalsLost))
				.value("ranking", QueryBuilder.raw(ranking));
		session.execute(insert.build());
	}

	public void updateClub() {
		Scanner scanner = new Scanner(System.in);

		System.out.println("Podaj id");
		String id = scanner.nextLine();

		System.out.println("Podaj nazwę klubu");
		String name = scanner.nextLine();

		System.out.println("Podaj liczbę punktów klubu");
		String points = scanner.nextLine();

		System.out.println("Podaj ilość strzelonych bramek");
		String goalsScored = scanner.nextLine();

		System.out.println("Podaj ilość straconych bramek");
		String goalsLost = scanner.nextLine();

		System.out.println("Podaj pozycję klubu w lidze");
		String ranking = scanner.nextLine();

		Update update = QueryBuilder.update("club")
				.setColumn("name", QueryBuilder.literal(name))
				.setColumn("points", QueryBuilder.literal(Integer.parseInt(points)))
				.setColumn("goals_scored", QueryBuilder.literal(Integer.parseInt(goalsScored)))
				.setColumn("goals_lost", QueryBuilder.literal(Integer.parseInt(goalsLost)))
				.setColumn("ranking", QueryBuilder.literal(Integer.parseInt(ranking)))
				.whereColumn("id").isEqualTo(QueryBuilder.literal(Integer.parseInt(id)));

		session.execute(update.build());
	}

	public void deleteClub() {
		System.out.println("Podaj id klubu który chcesz usunąć");
		Scanner scanner = new Scanner(System.in);
		String id = scanner.nextLine();

		Delete delete = QueryBuilder.deleteFrom("club").whereColumn("id").isEqualTo(QueryBuilder.literal(Integer.parseInt(id)));
		session.execute(delete.build());
	}

	public void findAll() {
		Select query = QueryBuilder.selectFrom("club").all();
		SimpleStatement statement = query.build();
		printResult(statement);
	}

	public void findClubById() {
		System.out.println("Podaj id klubu który chcesz znaleźć");

		Scanner scanner = new Scanner(System.in);
		String id = scanner.nextLine();

		Select query = QueryBuilder.selectFrom("club").all().whereColumn("id").isEqualTo(QueryBuilder.literal(Integer.parseInt(id)));
		SimpleStatement simpleStatement = query.build();
		printResult(simpleStatement);

	}

	public void sortClubDesc() {
		Select query = QueryBuilder.selectFrom("club").distinct().all().orderBy("points", ClusteringOrder.DESC);
		//Select query = Select
		SimpleStatement statement = query.build();
		printResult(statement);
	}

	public void findClubByName() {
		System.out.println("Podaj nazwę klubu");

		Scanner scanner = new Scanner(System.in);
		String clubName = scanner.nextLine();

		Select query = QueryBuilder.selectFrom("club").all().whereColumn("name").isEqualTo(QueryBuilder.literal(clubName)).allowFiltering();
		SimpleStatement statement = query.build();
		printResult(statement);
	}

	public void printResult(SimpleStatement simpleStatement) {
		ResultSet resultSet = session.execute(simpleStatement);
		for (Row row : resultSet) {
			System.out.print("Klub sportowy: ");
			System.out.print("id=" + row.getInt("id") + ", ");
			System.out.print("nazwa=" + row.getString("name") + ", ");
			System.out.print("punkty=" + row.getInt("points") + ", ");
			System.out.print("bramki strzelone=" + row.getInt("goals_scored") + ", ");
			System.out.print("bramki stracone=" + row.getInt("goals_lost") + ", ");
			System.out.print("pozycja=" + row.getInt("ranking"));
			System.out.println();
		}

		System.out.println("Statement \"" + simpleStatement.getQuery() + "\" executed successfully");
	}

	public void dropTable() {
		Drop drop = SchemaBuilder.dropTable("club");
		executeSimpleStatement(drop.build());
	}
}
