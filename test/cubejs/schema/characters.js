cube(`Characters`, {
    sql: `
          select * from characters
    `,

    joins: {
    },

    measures: {

      numberOfUsers: {
        type: `count`,
        description: `Number of the users`,
      },

      uniqueFirstNames: {
        sql: `firstname`,
        type: `countDistinct`,
        description: `Uniq first names`,
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
        title: `First name`,
        description: `First name of the character`,
      },

      lastname: {
        sql: `lastname`,
        type: `string`,
        title: `Last name`,
        description: `Last name of the character`,
      },

      active: {
        sql: `active`,
        type: `string`,
        description: `Is the user active?`
      },

      birth: {
        sql: `birth`,
        type: `time`,
        description: `Date of birth`
      }
    }
  });