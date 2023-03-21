"""
A simple example showing the generic JSON.

In this example, the JSON response looks like this:

    {
      "domains": [
        {
          "domain": "facebook-domain-verification8n83p6fzqi04yqdhyqb3vjpv73hzp9.com",
          "create_date": "2022-11-02T14:22:04.363028",
          "update_date": "2022-11-02T14:22:04.363030",
          "country": null,
          "isDead": "False",
          "A": null,
          "NS": null,
          "CNAME": null,
          "MX": null,
          "TXT": null
        },
        {
          "domain": "facebook-password-recover.com",
          "create_date": "2022-11-02T14:22:04.363231",
          "update_date": "2022-11-02T14:22:04.363233",
          "country": null,
          "isDead": "False",
          "A": null,
          "NS": null,
          "CNAME": null,
          "MX": null,
          "TXT": null
        },
        ...
      ],
      "total": 742,
      "time": "6",
      "next_page": null
    }

To get the actual data we need to specify the following JSONPath expression:

    $.domains[*]

"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = """
    SELECT domain, isDead FROM
    "https://api.domainsdb.info/v1/domains/search?domain=facebook&zone=com#$.domains[*]"
    WHERE isDead > 2
    ORDER BY domain DESC
    LIMIT 2
    """
    for row in cursor.execute(SQL):
        print(row)
