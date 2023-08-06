namespace ServiceBrokerListener.Domain;

#region Namespace imports

using System;
using System.IO;
using System.Xml;
using System.Xml.Linq;

#endregion

public class TableChangedEventArgs : EventArgs
{
    #region Constants and Fields

    private const string INSERTED_TAG = "inserted";

    private const string DELETED_TAG = "deleted";

    private static readonly XmlReaderSettings Settings = new() { CheckCharacters = false };

    private readonly string message;

    #endregion

    #region Constructors

    public TableChangedEventArgs(string message)
    {
        this.message = message;
    }

    #endregion

    #region Properties

    public XElement? Data
    {
        get
        {
            return string.IsNullOrWhiteSpace(this.message)
                ? null
                : ReadXDocumentWithInvalidCharacters(this.message);
        }
    }

    public NotificationTypes NotificationType
    {
        get
        {
            if (this.Data is null)
            {
                return NotificationTypes.None;
            }

            return
                this.Data.Element(INSERTED_TAG) is null
                ? this.Data.Element(DELETED_TAG) is null
                    ? NotificationTypes.None
                    : NotificationTypes.Delete
                : this.Data.Element(DELETED_TAG) is null
                    ? NotificationTypes.Insert
                    : NotificationTypes.Update;
        }
    }

    #endregion

    #region Private Methods

    /// <summary>
    /// Converts an xml string into XElement with no invalid characters check.
    /// https://paulselles.wordpress.com/2013/07/03/parsing-xml-with-invalid-characters-in-c-2/
    /// </summary>
    /// <param name="xml">The input string.</param>
    /// <returns>The result XElement.</returns>
    private static XElement? ReadXDocumentWithInvalidCharacters(string xml)
    {
        XDocument doc;

        using (StringReader sr = new(xml))
        using (var reader = XmlReader.Create(sr, Settings))
        {
            _ = reader.MoveToContent();
            doc = XDocument.Load(reader);
        }

        return doc.Root;
    }

    #endregion
}
