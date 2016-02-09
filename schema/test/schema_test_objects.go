package test

import s "github.com/twitchscience/scoop_protocol/schema"

func EventTest1() s.Event {
	return s.Event{
		EventName: "test_event_1",
		Version:   1,
		TableOption: s.TableOption{
			DistKey: []string{"test_event_1_outbound_col_1"},
		},
		Columns: []s.ColumnDefinition{
			s.ColumnDefinition{
				Transformer:           "varchar",
				InboundName:           "test_event_1_inbound_col_1",
				OutboundName:          "test_event_1_outbound_col_1",
				ColumnCreationOptions: "(500)",
			},
			s.ColumnDefinition{
				Transformer:           "varchar",
				InboundName:           "test_event_1_inbound_col_2",
				OutboundName:          "test_event_1_outbound_col_2",
				ColumnCreationOptions: "(500)",
			},
			s.ColumnDefinition{
				Transformer:           "varchar",
				InboundName:           "test_event_1_inbound_col_3",
				OutboundName:          "test_event_1_outbound_col_3",
				ColumnCreationOptions: "(500)",
			},
			s.ColumnDefinition{
				Transformer:           "varchar",
				InboundName:           "test_event_1_inbound_col_4",
				OutboundName:          "test_event_1_outbound_col_4",
				ColumnCreationOptions: "(500)",
			},
			s.ColumnDefinition{
				Transformer:           "varchar",
				InboundName:           "test_event_1_inbound_col_5",
				OutboundName:          "test_event_1_outbound_col_5",
				ColumnCreationOptions: "(500)",
			},
		},
		ParentMigration: s.Migration{},
	}
}

func EventTest1Empty() s.Event {
	return s.Event{
		EventName:       "test_event_1",
		Version:         1,
		ParentMigration: s.Migration{},
	}
}

func Migration1OnEvent1() s.Migration {
	return s.Migration{
		TableOperation: "add",
		Name:           "test_event_1",
		TableOption: s.TableOption{
			DistKey: []string{"test_event_1_new_outbound_col_1"},
		},
		ColumnOperations: []s.ColumnOperation{
			s.ColumnOperation{
				Operation:    "add",
				InboundName:  "test_event_1_new_inbound_col_1",
				OutboundName: "test_event_1_new_outbound_col_1",
				NewColumnDefinition: s.ColumnDefinition{
					Transformer:           "varchar",
					InboundName:           "test_event_1_new_inbound_col_1",
					OutboundName:          "test_event_1_new_outbound_col_1",
					ColumnCreationOptions: "(500)",
				},
			},
			s.ColumnOperation{
				Operation:    "add",
				InboundName:  "test_event_1_new_inbound_col_2",
				OutboundName: "test_event_1_new_outbound_col_2",
				NewColumnDefinition: s.ColumnDefinition{
					Transformer:           "varchar",
					InboundName:           "test_event_1_new_inbound_col_2",
					OutboundName:          "test_event_1_new_outbound_col_2",
					ColumnCreationOptions: "(500)",
				},
			},
			s.ColumnOperation{
				Operation:    "add",
				InboundName:  "test_event_1_new_inbound_col_3",
				OutboundName: "test_event_1_new_outbound_col_3",
				NewColumnDefinition: s.ColumnDefinition{
					Transformer:           "varchar",
					InboundName:           "test_event_1_new_inbound_col_3",
					OutboundName:          "test_event_1_new_outbound_col_3",
					ColumnCreationOptions: "(500)",
				},
			},
			s.ColumnOperation{
				Operation:    "add",
				InboundName:  "test_event_1_new_inbound_col_4",
				OutboundName: "test_event_1_new_outbound_col_4",
				NewColumnDefinition: s.ColumnDefinition{
					Transformer:           "varchar",
					InboundName:           "test_event_4_new_inbound_col_4",
					OutboundName:          "test_event_4_new_outbound_col_4",
					ColumnCreationOptions: "(500)",
				},
			},
		},
	}
}

func Migration2OnEvent1() s.Migration {
	return s.Migration{
		TableOperation:   "remove",
		Name:             "test_event_1",
		TableOption:      s.TableOption{},
		ColumnOperations: []s.ColumnOperation{},
	}
}

func Migration3OnEvent1() s.Migration {
	{
		return s.Migration{
			TableOperation: "update",
			Name:           "test_event_1",
			TableOption: s.TableOption{
				DistKey: []string{"test_event_1_outbound_col_1"},
			},
			ColumnOperations: []s.ColumnOperation{
				s.ColumnOperation{
					Operation:    "add",
					InboundName:  "test_event_1_new_inbound_col_1",
					OutboundName: "test_event_1_new_outbound_col_1",
					NewColumnDefinition: s.ColumnDefinition{
						Transformer:           "varchar",
						InboundName:           "test_event_1_new_inbound_col_1",
						OutboundName:          "test_event_1_new_outbound_col_1",
						ColumnCreationOptions: "(500)",
					},
				},
				s.ColumnOperation{
					Operation:    "update",
					InboundName:  "test_event_1_inbound_col_2",
					OutboundName: "test_event_1_outbound_col_2",
					NewColumnDefinition: s.ColumnDefinition{
						Transformer:           "varchar",
						InboundName:           "test_event_1_new_inbound_col_2_updated",
						OutboundName:          "test_event_1_new_outbound_col_2_updated",
						ColumnCreationOptions: "(500)",
					},
				},
				s.ColumnOperation{
					Operation:           "remove",
					InboundName:         "test_event_1_inbound_col_3",
					OutboundName:        "test_event_1_outbound_col_3",
					NewColumnDefinition: s.ColumnDefinition{},
				},
			},
		}
	}
}
