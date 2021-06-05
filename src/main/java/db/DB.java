/*
 * Copyright (C) 2021 Gustavo García <ggarciaa at gmail.com>
 *
 * Este programa es software libre: puede redistribuirlo o modificarlo
 * bajo los términos de la Licencia Pública General GNU publicada por
 * la Fundación del Software Libre, ya sea la versión 3 de la Licencia
 * u, opcionalmente, cualquier versión posterior.
 *
 * Este programa se distribuye con la esperanza de que sea útil,
 * pero SIN NINGUNA GARANTÍA, ni siquiera la garantía implícita de
 * UTILIDAD COMERCIAL o SU ADECUACIÖN A CUALQUIER PROPÓSITO PARTICULAR.
 * Ver la Licencia Pública General GNU para más detalles.
 *
 * Usted debería haber recibido una copia de la Licencia Pública
 * General GNU junto con este programa. En caso contrario, vea
 * <http://www.gnu.org/licenses/>.
 */
package db;

import com.mysql.cj.jdbc.MysqlDataSource;
import java.sql.Connection;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * La clase DB maneja la conexión a la base de datos, y las funcionalidades
 * necesarias para consultas de todo tipo.
 *
 * @author Gustavo García <ggarciaa at gmail.com>
 */
public class DB extends MysqlDataSource {

  /**
   * Nombre del archivo de configuración
   */
  final private static String DBPROPS = "dbprops.txt";
  /**
   * URL por defecto para el DataSource
   */
  final private static String PROTOCOL = "jdbc:mysql://";
  /**
   * La única instancia de esta clase (patrón singleton)
   */
  private static DB instancia = null;
  /**
   * La conexión efectiva al origen de datos.
   */
  private static Connection con = null;
  /**
   * Variable usada para enviar información a la clase principal
   */
  private static String msg = "";
  /**
   * Si es true, los métodos informan en el área de comunicaciones.
   */
  private static boolean informar;

  /**
   * El constructor de la clase MysqlDataSource crea un objeto en blanco.
   * <p>
   * Si los datos de acceso estan mal no tira excepciones.
   *
   * @throws IOException
   * @throws SQLException
   */
  private DB() throws IOException, SQLException {
    super();
    /**
     * Para leer el archivo de configuración.
     */
    FileInputStream fis;
    /**
     * Para guardar los parámetros de la base de datos.
     */
    Properties props;
    /**
     * El constructor de la clase Properties tampoco tira excepciones.
     */
    props = new Properties();
    /**
     * Leer los valores de un archivo de configuración (mejor práctica).
     */
    try {
      /*
       * El constructor FileInputStream(String) puede tirar la excepción
       * FileNotFoundException - if the file does not exist, is a directory
       * rather than a regular file, or for some other reason cannot be opened
       * for reading.
       */
      fis = new FileInputStream(DBPROPS);
      /*
       * El archivo existe. Cargo las propiedades desde él.
       */
      props.load(fis);
      setURL(PROTOCOL + props.getProperty("HOST")
              + "/" + props.getProperty("DATABASE"));
      setUser(props.getProperty("USER"));
      setPassword(props.getProperty("PASSWORD"));
      if (informar) {
        msg += "setDSProps() valores de archivo de configuración.\n"
                + System.getProperty("user.dir") + "\n";
      }
      /*
       * Trato de conectarme a la base de datos. Si la conexión falla, voy a
       * tener una excepción, y se la paso a getInstancia(). Si no hay
       * excepción, está todo bien. En este caso, getConnection() me retorna un
       * objeto de clase Connection, pero como no lo asigno, simplemente se
       * resetea a null al quedar fuera de scope cuando termina el constructor.
       */
      getConnection();
    } catch (FileNotFoundException e) {
      msg += "El archivo de configuración NO existe...";
    }
  }

  /**
   * Borra el contenido de los mensajes.
   */
  public static void clearMsg() {
    msg = "";
  }

  /**
   * Trata de conectarse a la base de datos.
   * <p>
   * Si ya existe una conexión, retorna true. En caso contrario, trata de
   * conectarse a la base de datos. Si existe una instancia válida de la clase
   * DB, todos los parámetros ya fueron validados por el constructor. Por lo
   * tanto trata de conectarse. Si la conexión es exitosa, retorna true. Si no
   * se puede conectar, anula la única instancia de esta clase DB. Esto permite
   * reintentar con otros parámetros, o con los mismos, sin tener que reniniciar
   * la aplicación.
   *
   * @return void
   */
  public static boolean conectar() {
    if (con == null) {
      try {
        /*
         * getConnection() throws: SQLException - if a database access error
         * occurs SQLTimeoutException - when the driver has determined that the
         * timeout value specified by the setLoginTimeout method has been
         * exceeded and has at least tried to cancel the current database
         * connection attempt. Esta es una subclase de la primera de modo que el
         * catch las agarra a las dos.
         */
        if (getInstancia() == null) {
          return false;
        }
        con = instancia.getConnection();
        if (informar) {
          msg += "Conectado con la base de datos.\n";
        }
      } catch (SQLException e) {
        /*
         * Antes de anular la instancia de la clase DB, tomo la precaución de
         * anular las referencias a todos los objetos que son propiedades de DB
         * y yo inicialicé. Esto es un modo de prevenir pérdidas de memoria.
         */
        con = null;
        msg = null;
        instancia = null;
        if (informar) {
          msg += "No se pudo establecer la conexión con la base de datos.\n";
        }
        return false;
      }
    } else {
      if (informar) {
        msg += "Ya existe una conexión con la base de datos.\n";
      }
    }
    return true;
  }

  /**
   * Consulta la tabla empleado.
   * <p>
   * @return void
   */
  public static void consultar() {
    Statement stmt = null;
    ResultSet rs = null;
    if (!conectar()) {
      return;
    }
    try {
      stmt = con.createStatement();
      rs = stmt.executeQuery(
              "select id, nombre, apellido, profesion from empleado");
      while (rs.next()) {
      Array datos = rs.getArray("Datos");
          String[] data = (String[])datos.getArray();
          System.out.println(data);
        msg += String.format("%03d %s %s %s%n",
                rs.getInt("id"),
                rs.getString("nombre"), rs.getString("apellido"),
                rs.getString("profesion"));
      }
    } catch (SQLException e) {
      msg += e.toString();
    }
    /*
     * Liberamos recursos y evitamos memory leaks.
     */
    try {
      if (rs != null) {
        rs.close();
      }
      if (stmt != null) {
        stmt.close();
      }
    } catch (SQLException e) {
      msg += e.toString();
    }
    desconectar();
  }

  /**
   * Desconectar la base de datos.
   * <p>
   * Esto es solo para liberar recursos. No anulo los objetos que son válidos, y
   * pertenecen exclusivamente a esta aplicación, porque no molestan a otros
   * usuarios. Si necesito usar de nuevo la base de datos, lo único que tengo
   * que hacer es usar el método conectar()
   *
   * @return void
   */
  public static void desconectar() {
    if (con != null) {
      try {
        con.close();
        con = null;
        if (informar) {
          msg += "Cerrada la conexión a la base de datos.\n";
        }
      } catch (SQLException e) {
        if (informar) {
          msg += "Problemas cerrando la conexión a la base de datos.\n"
                  + e.toString();
        }
      }
    } else {
      if (informar) {
        msg += "No existe una conexión a la base de datos.\n";
      }
    }
  }

  // Si ya existe una instancia válida, simplemente la retorna.
  // En caso contrario, intenta construirla.
  // Si todo sale bien, incluida la conexión a la base de datos,
  // la única instancia de la clase DB es construida.
  // Si ocurre alguna excepción, la única instancia de la
  // clase DB es puesta a null.
  // Retorna la instancia, sea null o distinta de null.
  public static DB getInstancia() {
    if (instancia == null) {
      try {
        instancia = new DB();
      } catch (IOException ex) {
        // Excepción tirada por la función 
        // new FileInputStream(DBPROPS) del constructor
        instancia = null;
        if (informar) {
          msg += " Error leyendo archivo de configuración.";
        }
      } catch (SQLException ex) {
        // Excepción tirada por la función getConnection() del constructor
        instancia = null;
        if (informar) {
          msg += " Error de conexión con la base de datos.";
        }
      }
    }
    return instancia;
  }

  public static String getMsg() {
    return msg;
  }

  public static void grabarIdea(String ideaBrillante, boolean informar) {
    Statement stmt = null;
    int rowCount = 0;
    if (ideaBrillante.length() == 0) {
      if (informar) {
        msg += "No hay nada escrito.";
      }
      return;
    }
    if (!conectar()) {
      return;
    }
    try {
      stmt = con.createStatement();
      String str = "insert into ideabrillante (id, ideabrillante)"
              + " values (null, '"
              + ideaBrillante
              + "')";
      rowCount = stmt.executeUpdate(str);
    } catch (SQLException e) {
      if (informar) {
        msg += (e.toString() + "\n");
      }
    } catch (Exception e) {
      if (informar) {
        msg += "Error occurred in inserting data.\n";
        e.printStackTrace();
      }
    }
    if (informar) {
      if (rowCount == 1) {
        msg += "Se ha grabado la idea brillante.\n";
      } else {
        msg += "Error grabando la idea brillante.\n";
      }
    }
    desconectar();
  }

  public static void grabarIdea(String ideaBrillante) {
    if (ideaBrillante.length() == 0) {
      if (informar) {
        msg += "No hay nada escrito.";
      }
      return;
    }
    grabarIdea(ideaBrillante, informar);
  }

  public static void setInformar(boolean infor) {
    informar = infor;
  }

}
