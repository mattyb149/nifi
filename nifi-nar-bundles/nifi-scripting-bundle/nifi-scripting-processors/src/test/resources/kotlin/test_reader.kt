
class KotlinProcessor : AbstractProcessor() {

    val REL_TEST = Relationship.Builder()
            .name("test")
            .description("A test relationship")
            .build()

    val descriptor = PropertyDescriptor.Builder()
            .name("test-attribute")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build()


    override fun getRelationships() : Set<Relationship> {
        return setOf(REL_TEST)
    }

    @Throws(ProcessException::class)
    override fun onTrigger(context : ProcessContext, session : ProcessSession) {
        var flowFile: FlowFile? = session?.get() ?: return
        flowFile = session.putAttribute(flowFile, "from-content", "test content")
        // transfer
        session.transfer(flowFile, REL_TEST)
        session.commit()
    }


    override fun getSupportedPropertyDescriptors() : List<PropertyDescriptor> {
        return listOf(descriptor)
    }
}
val processor = KotlinProcessor()
(bindings as Bindings).put("processor", processor)
processor