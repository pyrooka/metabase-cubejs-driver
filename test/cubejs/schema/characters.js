cube(`Characters`, {
    sql: `
          select * from characters
    `,

    joins: {
    },

    measures: {

      numberOfUsers: {
        type: "count",
      },
    },

    dimensions: {

      countrycode: {
        sql: `countrycode`,
        type: `string`,
        title: `Country code`
      },

      firstname: {
        sql: `firstname`,
        type: `string`,
        title: `First name`
      },

      lastname: {
        sql: `lastname`,
        type: `string`,
        title: `Last name`
      },

      active: {
        sql: `active`,
        type: `string`,
        title: `Is the user active?`
      },

      birth: {
        sql: `birth`,
        type: `time`,
        title: `Date of birth`
      }
    }
  });