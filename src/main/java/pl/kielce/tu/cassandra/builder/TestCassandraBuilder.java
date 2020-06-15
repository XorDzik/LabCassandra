package pl.kielce.tu.cassandra.builder;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Scanner;

public class TestCassandraBuilder {
	public static void main(String[] args) {
		try (CqlSession session = CqlSession.builder().build()) {
			KeyspaceBuilderManager keyspaceManager = new KeyspaceBuilderManager(session, "sports_league");
			keyspaceManager.dropKeyspace();
			keyspaceManager.selectKeyspaces();
			keyspaceManager.createKeyspace();
			keyspaceManager.useKeyspace();

			System.out.println();
			System.out.println();
			System.out.println("Aplikacja ma za zadanie pokazać działanie składu Cassandra");
			System.out.println("Temat: Liga sportowa");
			StudentsTableBuilderManager tableManager = new StudentsTableBuilderManager(session);
			tableManager.createTable();

			for (;;) {
				System.out.println("1 -> Dodaj klub");
				System.out.println("2 -> Zaktualizuj informacje o klubie");
				System.out.println("3 -> Usuń klub po id");
				System.out.println("4 -> Wyświetl wszystkie kluby");
				System.out.println("5 -> Znajdź klub po id");
				System.out.println("6 -> Znajdź klub po nazwie");
				System.out.println("7 -> Posortuj kluby po ilości punktów malejąco");
				System.out.println("8 -> Zakończ program");

				Scanner scanner = new Scanner(System.in);
				String choice = scanner.nextLine();

				if (choice.equals("1"))
					tableManager.addClub();

				if (choice.equals("2"))
					tableManager.updateClub();

				if (choice.equals("3"))
					tableManager.deleteClub();

				if (choice.equals("4"))
					tableManager.findAll();

				if (choice.equals("5"))
					tableManager.findClubById();

				if (choice.equals("6"))
					tableManager.findClubByName();

				if (choice.equals("7"))
					tableManager.sortClubDesc();

				if (choice.equals("8"))
					break;
			}
			tableManager.dropTable();
		}
	}
}